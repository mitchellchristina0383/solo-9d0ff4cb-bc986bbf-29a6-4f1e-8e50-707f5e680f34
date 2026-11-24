# -*- coding: utf-8 -*-
"""文件存储层。

三个等容量“分区”，每个分区是一个目录（容器里通常是挂载的 loop 镜像）：

    <mount>/recordings/<rid>.bin   正常录像文件
    <mount>/staging/<rid>.part     回收搬移时的临时文件（完整后 rename 成 moved）
    <mount>/moved/<rid>.bin        搬入的留存片段

容量限制由应用按元数据记账强制执行（df 对 loop 镜像同样可用，
但记账方式在目录降级模式下也保持一致）。
"""
from __future__ import annotations

import hashlib
import os
import shutil
from typing import Optional

CHUNK = 1024 * 1024  # 1 MiB

# ---- 故障注入（仅 ENABLE_TEST_HOOKS=1 时生效）----
# move_fail_after: 本次回收累计搬移写入多少字节后抛 IOError
#                  （跨文件累计，可精确模拟“第二个文件搬到一半失败”）
_FAIL_FLAGS = {"move_fail_after": None}
_MOVE_BYTES = 0  # 全局累计：一次回收周期内所有 copy_with_verify 的写入量


def configure_failure(move_fail_after: Optional[int] = None) -> None:
    _FAIL_FLAGS["move_fail_after"] = move_fail_after


def failure_config() -> dict:
    return dict(_FAIL_FLAGS)


def reset_failure() -> None:
    global _MOVE_BYTES
    _FAIL_FLAGS["move_fail_after"] = None
    _MOVE_BYTES = 0


def reset_move_counter() -> None:
    """每次回收开始时调用，让故障阈值按本轮累计字节计算。"""
    global _MOVE_BYTES
    _MOVE_BYTES = 0


def rec_path(mount_path: str, rid: str) -> str:
    return os.path.join(mount_path, "recordings", f"{rid}.bin")


def staging_path(mount_path: str, rid: str, source_id: str = "") -> str:
    # 按回收源分区隔离，清理某分区的失败残留时不会波及其他批次
    if source_id:
        d = os.path.join(mount_path, "staging", source_id)
        return os.path.join(d, f"{rid}.part")
    return os.path.join(mount_path, "staging", f"{rid}.part")


def moved_path(mount_path: str, rid: str) -> str:
    return os.path.join(mount_path, "moved", f"{rid}.bin")


def relpath_for(rid: str, bucket: str) -> str:
    return f"{bucket}/{rid}.bin"


def abs_path(mount_path: str, relpath: str) -> str:
    return os.path.join(mount_path, relpath)


def ensure_partition_dirs(mount_path: str) -> None:
    for sub in ("recordings", "staging", "moved"):
        os.makedirs(os.path.join(mount_path, sub), exist_ok=True)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(CHUNK)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def save_upload(mount_path: str, rid: str, stream, declared_size: int,
                declared_sha256: Optional[str]) -> tuple[int, str, str]:
    """流式保存上传，落盘后回读校验大小与 sha256。返回 (size, sha256, relpath)。"""
    ensure_partition_dirs(mount_path)
    final = rec_path(mount_path, rid)
    tmp = final + ".uploading"
    h = hashlib.sha256()
    size = 0
    try:
        with open(tmp, "wb") as out:
            while True:
                buf = stream.read(CHUNK)
                if not buf:
                    break
                out.write(buf)
                h.update(buf)
                size += len(buf)
        os.replace(tmp, final)
    except BaseException:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise

    digest = h.hexdigest()
    if declared_size is not None and size != declared_size:
        os.remove(final)
        raise IOError(f"上传大小不符: 声明 {declared_size}，实得 {size}")
    if declared_sha256 and digest != declared_sha256:
        os.remove(final)
        raise IOError("上传 sha256 校验失败")
    # 回读复核（防止半截写入被当成完整文件）
    if sha256_file(final) != digest:
        os.remove(final)
        raise IOError("落盘后回读校验失败")
    return size, digest, relpath_for(rid, "recordings")


def copy_with_verify(src: str, dst_tmp: str, expected_size: int,
                     expected_sha256: str, counter: Optional[list] = None) -> None:
    """分块复制到 .part 临时文件，模拟故障可在写入中途触发。

    成功后调用方负责 rename；任何异常都由调用方清理 dst_tmp。
    整个过程不动 src，保证原目录和原录像一直可用。
    """
    global _MOVE_BYTES
    fail_after = _FAIL_FLAGS.get("move_fail_after")
    written = 0
    h = hashlib.sha256()
    os.makedirs(os.path.dirname(dst_tmp), exist_ok=True)
    with open(src, "rb") as fin, open(dst_tmp, "wb") as fout:
        while True:
            buf = fin.read(CHUNK)
            if not buf:
                break
            fout.write(buf)
            fout.flush()
            os.fsync(fout.fileno())
            h.update(buf)
            written += len(buf)
            _MOVE_BYTES += len(buf)
            if counter is not None:
                counter[0] = written
            if fail_after is not None and _MOVE_BYTES >= fail_after:
                raise IOError(
                    f"故障注入: 累计搬移 {_MOVE_BYTES} 字节后写入失败"
                )
    if written != expected_size:
        raise IOError(f"搬移大小不符: 期望 {expected_size}，实写 {written}")
    if h.hexdigest() != expected_sha256:
        raise IOError("搬移后哈希校验失败（临时文件内容损坏）")


def commit_moved_file(mount_path: str, rid: str, source_id: str = "") -> str:
    """staging/.../<rid>.part 完整且校验通过后，原子 rename 到 moved/<rid>.bin。"""
    tmp = staging_path(mount_path, rid, source_id)
    final = moved_path(mount_path, rid)
    os.makedirs(os.path.dirname(final), exist_ok=True)
    os.replace(tmp, final)
    return relpath_for(rid, "moved")


def cleanup_staging(mount_path: str, source_id: str = "") -> int:
    """清理搬移残留（.part），返回清理文件数。

    指定 source_id 时只清理该回收批次的隔离目录；
    不传则清理全部 staging（兜底用）。
    """
    sroot = os.path.join(mount_path, "staging")
    if not os.path.isdir(sroot):
        return 0
    removed = 0
    if source_id:
        sdir = os.path.join(sroot, source_id)
        if not os.path.isdir(sdir):
            return 0
        for name in os.listdir(sdir):
            p = os.path.join(sdir, name)
            if name.endswith(".part") and os.path.isfile(p):
                try:
                    os.remove(p)
                    removed += 1
                except OSError:
                    pass
        try:
            # 空目录一并删掉
            os.rmdir(sdir)
        except OSError:
            pass
        return removed
    for root, _dirs, files in os.walk(sroot):
        for name in files:
            if name.endswith(".part"):
                try:
                    os.remove(os.path.join(root, name))
                    removed += 1
                except OSError:
                    pass
    return removed


def wipe_partition(mount_path: str) -> None:
    """清空分区上的全部数据（仅允许在无活动下载时由业务层调用）。"""
    for sub in ("recordings", "staging", "moved"):
        d = os.path.join(mount_path, sub)
        if os.path.isdir(d):
            shutil.rmtree(d)
    ensure_partition_dirs(mount_path)


def free_bytes(mount_path: str, capacity: int) -> int:
    """按元数据记账的剩余容量（同时参考文件系统真实剩余空间，取较小值）。"""
    used = 0
    for sub in ("recordings", "staging", "moved"):
        d = os.path.join(mount_path, sub)
        if not os.path.isdir(d):
            continue
        for name in os.listdir(d):
            p = os.path.join(d, name)
            if os.path.isfile(p):
                used += os.path.getsize(p)
    st = os.statvfs(mount_path)
    fs_free = st.f_bavail * st.f_frsize
    return max(0, min(capacity - used, fs_free))
