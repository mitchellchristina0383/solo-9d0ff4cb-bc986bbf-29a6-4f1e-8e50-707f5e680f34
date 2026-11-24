# -*- coding: utf-8 -*-
"""业务服务层：分区生命周期、上传、封区、回收搬移、排空清理。

状态机：
    idle      --(提升为写入区)-->      active
    active    --(封区)-->              sealed
    sealed    --(发起回收)-->          recycling
    recycling --(搬移完成)-->          draining     （等活动下载结束）
    recycling --(搬移失败)-->          recycling    （原数据不动，可清理周转区后重试）
    draining  --(活动下载归零、清空)--> idle         （回到周转池，留给下次周转）
"""
from __future__ import annotations

import os
import threading
import time
from typing import Optional

from . import db, storage
from .config import Config

# 同一时刻只允许一个回收任务（针对不同分区串行，避免周转区记账竞争）
_RECYCLE_LOCK = threading.Lock()
# 清空操作串行化
_WIPE_LOCK = threading.Lock()


class ServiceError(Exception):
    """业务错误，message 可直接展示给网页。"""


# ---------------- 初始化 ----------------

def bootstrap() -> None:
    """确保三个等容量分区存在。首次启动时 p1 为 active，p2/p3 为 idle 周转区。"""
    existing = db.list_partitions()
    if existing:
        for p in existing:
            storage.ensure_partition_dirs(p["mount_path"])
        return

    data_dir = Config.DATA_DIR
    os.makedirs(data_dir, exist_ok=True)
    for seq in range(1, Config.PARTITION_COUNT + 1):
        pid = f"p{seq}"
        mount = os.path.join(data_dir, pid)
        storage.ensure_partition_dirs(mount)
        image = os.path.join(data_dir, f"{pid}.img")
        state = "active" if seq == 1 else "idle"
        db.upsert_partition(pid, seq, state, Config.PARTITION_CAPACITY, mount,
                            image if os.path.exists(image) else None)
    db.log_event("info", "bootstrap",
                 f"初始化 {Config.PARTITION_COUNT} 个等容量分区，"
                 f"单区容量 {Config.PARTITION_CAPACITY} 字节，p1 为写入区")


# ---------------- 分区视图 ----------------

def partition_view(p) -> dict:
    used = db.partition_used_bytes(p["id"])
    active_dl = db.active_download_count(p["id"])
    return {
        "id": p["id"],
        "seq": p["seq"],
        "state": p["state"],
        "capacity": p["capacity"],
        "used": used,
        "free": max(0, p["capacity"] - used),
        "mount_path": p["mount_path"],
        "image_path": p["image_path"],
        "last_error": p["last_error"],
        "active_downloads": active_dl,
        "recording_count": len(db.recordings_on_partition(p["id"])),
    }


def overview() -> dict:
    parts = [partition_view(p) for p in db.list_partitions()]
    active = next((p for p in parts if p["state"] == "active"), None)
    spare = [p["id"] for p in parts if p["state"] == "idle"]
    return {
        "partitions": parts,
        "active_id": active["id"] if active else None,
        "spare_ids": spare,
        "recordings": [dict(r) for r in db.list_recordings()],
        "active_downloads": [dict(d) for d in db.list_active_downloads()],
        "events": [dict(e) for e in db.list_events(80)],
        "failure": storage.failure_config() if Config.ENABLE_TEST_HOOKS else None,
    }


# ---------------- 上传 ----------------

def add_recording(filename: str, stream, declared_size: Optional[int] = None,
                  declared_sha256: Optional[str] = None,
                  retained: bool = False) -> dict:
    """把一段短录像追加到当前 active 分区；容量不足直接拒绝，不落半截文件。"""
    with db.lock():
        active = db.find_partition("active")
        if active is None:
            raise ServiceError("没有可写入的 active 分区，请先封区回收或提升周转区")
        pid = active["id"]
        mount = active["mount_path"]
        capacity = active["capacity"]
        used = db.partition_used_bytes(pid)

    if declared_size is not None and used + declared_size > capacity:
        raise ServiceError(
            f"分区 {pid} 剩余 {capacity - used} 字节，"
            f"装不下 {declared_size} 字节的录像"
        )

    rid = db.new_id("rec")
    try:
        size, digest, relpath = storage.save_upload(
            mount, rid, stream, declared_size, declared_sha256
        )
    except Exception as exc:  # 落盘失败：临时文件已在 storage 内清理
        raise ServiceError(f"写入分区 {pid} 失败: {exc}") from exc

    # 记账复核（防止超额）
    with db.lock():
        used_after = db.partition_used_bytes(pid) + size
        if used_after > capacity:
            try:
                os.remove(storage.abs_path(mount, relpath))
            except OSError:
                pass
            raise ServiceError(f"分区 {pid} 容量超限，已拒绝该录像")
        db.insert_recording({
            "id": rid,
            "filename": filename,
            "size": size,
            "sha256": digest,
            "retained": retained,
            "source_partition": pid,
            "current_partition": pid,
            "relpath": relpath,
        })
        rec = dict(db.get_recording(rid))
    db.log_event("info", "upload",
                 f"录像 {filename}({rid}) 写入 {pid}，{size} 字节")
    return rec


def set_retained(rid: str, retained: bool) -> dict:
    rec = db.get_recording(rid)
    if rec is None:
        raise ServiceError("录像不存在")
    db.set_retained(rid, retained)
    db.log_event("info", "retain",
                 f"录像 {rid} 标记为{'留存' if retained else '取消留存'}")
    return dict(db.get_recording(rid))


# ---------------- 下载准入 ----------------

def resolve_for_download(rid: str) -> tuple:
    """返回 (rec_row, partition_row, abs_file_path)；不允许则抛 ServiceError。

    规则：
    - 已开始的下载按 token 继续读原字节（在 download 流式处处理，不走本函数）；
    - recycling/draining 分区上“未留存”的片段停止接受新下载；
    - 留存片段搬到周转/新写入区后，原链接通过新位置取录像。
    """
    rec = db.get_recording(rid)
    if rec is None:
        raise ServiceError("录像不存在")
    part = db.get_partition(rec["current_partition"])
    if part is None:
        raise ServiceError("录像所在分区不存在")

    if part["state"] in ("recycling", "draining") and not rec["retained"]:
        raise ServiceError(
            f"分区 {part['id']} 已封区回收，未留存片段停止接受新下载"
        )
    path = storage.abs_path(part["mount_path"], rec["relpath"])
    if not os.path.isfile(path):
        raise ServiceError("录像文件缺失（元数据与磁盘不一致）")
    return rec, part, path


# ---------------- 封区 / 回收 ----------------

def seal_active() -> dict:
    """封掉当前 active 分区，并从周转池提升一个 idle 分区作为新的写入区。"""
    with _RECYCLE_LOCK:
        with db.lock():
            active = db.find_partition("active")
            if active is None:
                raise ServiceError("当前没有 active 分区")
            spare = db.find_partition("idle")
            if spare is None:
                raise ServiceError(
                    "没有空闲周转分区，无法封区：请先完成在途回收，腾出周转区"
                )
            old_id, new_id = active["id"], spare["id"]
            db.set_partition_state(old_id, "sealed")
            db.set_partition_state(new_id, "active")
        db.log_event("info", "seal",
                     f"分区 {old_id} 已封区（停止新写入），"
                     f"周转分区 {new_id} 提升为新的写入区，新录像可继续追加")
        return {"sealed": old_id, "new_active": new_id}


def start_recycle(source_id: Optional[str] = None) -> dict:
    """对 sealed 分区发起回收：留存片段全搬到当前写入区（搬入区）。

    - 搬移逐文件 copy→校验→原子 rename→切换元数据；
    - 任一文件失败即中止：原目录/原录像不动，已搬入的保留，
      清理周转区残留后可幂等重试；
    - 成功后源分区转 draining，等在途下载结束再清空回 idle。
    """
    if not _RECYCLE_LOCK.acquire(blocking=False):
        raise ServiceError("已有回收任务在执行，请等待其结束")
    try:
        with db.lock():
            source = db.find_partition("sealed") if source_id is None \
                else db.get_partition(source_id)
            if source is None:
                raise ServiceError("没有可回收的 sealed 分区")
            if source["state"] not in ("sealed", "recycling"):
                raise ServiceError(
                    f"分区 {source['id']} 状态为 {source['state']}，不能回收"
                )
            target = db.find_partition("active")
            if target is None:
                raise ServiceError("没有 active 搬入区，无法回收")
            if target["id"] == source["id"]:
                raise ServiceError("搬入区不能与被回收分区相同")
            sid, tid = source["id"], target["id"]
            db.set_partition_state(sid, "recycling")
            storage.reset_move_counter()

        db.log_event("info", "recycle", f"开始回收分区 {sid}，留存片段搬入 {tid}")
        retained = db.recordings_on_partition(sid, retained=True)
        moved, failed = [], []
        for rec in retained:
            rid = rec["id"]
            # 幂等：上一轮已搬好的跳过
            if rec["current_partition"] == tid:
                moved.append(rid)
                continue
            src = storage.abs_path(source["mount_path"], rec["relpath"])
            tmp = storage.staging_path(target["mount_path"], rid, sid)
            try:
                # 搬入区容量预检
                need = rec["size"]
                if storage.free_bytes(target["mount_path"], target["capacity"]) < need:
                    raise ServiceError(
                        f"搬入区 {tid} 空间不足，无法容纳 {rid}（需 {need} 字节）"
                    )
                storage.copy_with_verify(
                    src, tmp, rec["size"], rec["sha256"]
                )
                new_rel = storage.commit_moved_file(target["mount_path"], rid, sid)
                db.relocate_recording(rid, tid, new_rel)
                moved.append(rid)
                db.log_event("info", "move",
                             f"留存片段 {rid} 已搬到 {tid}:{new_rel}，原链接已切换")
            except Exception as exc:
                # 清掉这次失败的半截临时文件；源文件与已提交的搬移均保持不动
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except OSError:
                    pass
                failed.append({"rid": rid, "error": str(exc)})
                db.set_partition_error(sid, str(exc))
                db.log_event("error", "move",
                             f"片段 {rid} 搬移失败: {exc}；原目录与原录像照常可用")
                break

        if failed:
            return {
                "status": "failed",
                "source": sid,
                "target": tid,
                "moved": moved,
                "failed": failed,
            }

        with db.lock():
            db.set_partition_state(sid, "draining", error=None)
            unmarked = db.recordings_on_partition(sid, retained=False)
        db.log_event(
            "info", "recycle",
            f"分区 {sid} 的 {len(moved)} 个留存片段全部搬到 {tid}；"
            f"{len(unmarked)} 个未留存片段停止接受新下载，"
            f"分区进入 draining，等待 {db.active_download_count(sid)} 个在途下载结束"
        )
        # 立即尝试排空（可能没有在途下载）
        drain_result = try_drain(sid)
        return {
            "status": "draining",
            "source": sid,
            "target": tid,
            "moved": moved,
            "drain": drain_result,
        }
    finally:
        _RECYCLE_LOCK.release()


def cleanup_turnover(source_id: str) -> dict:
    """回收失败后清理周转区（搬入区）的 .part 残留，并把已切换归属的录像
    回滚到源分区（源文件从未删除），使系统回到可重试状态。"""
    source = db.get_partition(source_id)
    if source is None:
        raise ServiceError("分区不存在")
    if source["state"] != "recycling":
        raise ServiceError(f"分区 {source_id} 不在 recycling 状态，无需清理")
    target = db.find_partition("active")
    removed = 0
    rolled_back = []
    if target is not None:
        removed = storage.cleanup_staging(target["mount_path"], source_id)
        # 回滚本轮/上一轮已提交到搬入区、但源分区仍处于 recycling 的录像归属，
        # 同时删除搬入区中的孤儿副本，使系统完全回到回收前状态。
        # 注意：搬入区上的片段可能来自更早的分区（链式周转），
        # 不能用 source_partition 限定。
        for rec in db.list_recordings():
            if rec["current_partition"] != target["id"] or rec["state"] != "moved":
                continue
            # 只要该录像的源分区仍在 recycling（即本次回收尚未成功），
            # 就需要回滚；不要求源分区编号等于当前操作的 source_id，
            # 以覆盖链式周转中片段来自更早分区的情况。
            owner = db.get_partition(rec["source_partition"])
            if owner is None or owner["state"] != "recycling":
                continue
            orphan = storage.abs_path(target["mount_path"], rec["relpath"])
            try:
                if os.path.isfile(orphan):
                    os.remove(orphan)
            except OSError:
                pass
            src_rel = storage.relpath_for(rec["id"], "recordings")
            src_abs = storage.abs_path(owner["mount_path"], src_rel)
            if os.path.isfile(src_abs):
                db.reset_recording_location(rec["id"], owner["id"], src_rel)
                rolled_back.append(rec["id"])
    db.set_partition_error(source_id, None)
    db.log_event("info", "cleanup",
                 f"已清理周转区残留 {removed} 个临时文件，"
                 f"回滚 {len(rolled_back)} 个录像归属，可重新发起回收")
    return {"removed_parts": removed, "rolled_back": rolled_back}


def try_drain(partition_id: str) -> dict:
    """draining 分区在途下载归零后清空数据，回到 idle 周转池。"""
    part = db.get_partition(partition_id)
    if part is None or part["state"] != "draining":
        return {"partition": partition_id, "action": "skip",
                "reason": "not draining"}
    active = db.active_download_count(partition_id)
    if active > 0:
        return {"partition": partition_id, "action": "waiting",
                "active_downloads": active}
    # 注意：本函数可能被 start_recycle 在持有 _RECYCLE_LOCK 时内部调用，
    # 因此这里不能再去获取 _RECYCLE_LOCK（非重入会自死锁）。
    # 状态/计数在元数据锁内重读 + WIPE 双检已足以保证安全。
    with _WIPE_LOCK:
        # 锁内重读状态与计数（准入逻辑已拒绝新下载，双检仅作保险）
        part = db.get_partition(partition_id)
        if part is None or part["state"] != "draining":
            return {"partition": partition_id, "action": "skip",
                    "reason": "state changed"}
        active = db.active_download_count(partition_id)
        if active > 0:
            return {"partition": partition_id, "action": "waiting",
                    "active_downloads": active}
        storage.wipe_partition(part["mount_path"])
        deleted = db.delete_recordings_on_partition(partition_id)
        db.set_partition_state(partition_id, "idle", error=None)
    db.log_event("info", "drain",
                 f"分区 {partition_id} 在途下载已全部结束，数据清空"
                 f"（删除 {deleted} 条未留存录像元数据），"
                 f"回到 idle 周转池，留给下次周转")
    return {"partition": partition_id, "action": "cleared", "state": "idle"}


def drain_loop(stop_event: threading.Event) -> None:
    """后台线程：周期性排空所有 draining 分区。"""
    while not stop_event.wait(Config.DRAIN_POLL_INTERVAL):
        try:
            for p in db.list_partitions():
                if p["state"] == "draining":
                    try_drain(p["id"])
        except Exception as exc:  # 后台线程绝不能死掉
            db.log_event("error", "drain_loop", f"排空轮询异常: {exc}")
