# -*- coding: utf-8 -*-
"""端到端验收脚本（不依赖 pytest，直接 python 运行）。

用法：
    BASE_URL=http://127.0.0.1:8080 python -I tests/e2e_test.py

覆盖：
  1. 上传/容量/下载/Range/标记留存基础流程
  2. 封区 + 周转分区提升为新写入区，新录像可继续追加
  3. 回收：留存片段搬入写入区，原链接从新位置取录像；
     未留存片段停止接受新下载；
  4. 下载中回收：慢速下载未结束时源分区保持 draining 不被清空，
     下载读完原字节结束后分区自动清空回 idle；
  5. 搬移失败注入：原目录与原录像照常可用，清理周转区后重试成功。
  6. 封区 vs 在途上传竞态（上传闸门确定性复现）：
     a) 选中旧写入区、登记前封区 + 回收：上传报错、文件被清理、不入列表、不占容量；
     b) 旧区封区后又被重新启用：早先选中的上传仍然报错（代次轮换）；
     c) 只封区不回收：被拒上传写出的文件从旧区磁盘删除；
     d) 封区/回收无需等待上传结束，新上传照常写入新写入区。
"""
import hashlib
import json
import os
import sys
import threading
import time
import urllib.request
import urllib.error
import uuid

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8080").rstrip("/")
PASS, FAIL = 0, 0
RESULTS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    tag = "PASS" if cond else "FAIL"
    if cond:
        PASS += 1
    else:
        FAIL += 1
    line = f"[{tag}] {name}" + (f" -- {detail}" if detail else "")
    print(line, flush=True)
    RESULTS.append((tag, name, detail))


def req(method: str, path: str, data=None, headers=None, raw=False,
        timeout=30):
    url = BASE + path
    body = None
    h = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        body = json.dumps(data).encode()
        h.setdefault("Content-Type", "application/json")
    elif data is not None:
        body = data
    r = urllib.request.Request(url, data=body, headers=h, method=method)
    try:
        resp = urllib.request.urlopen(r, timeout=timeout)
        payload = resp.read()
        if raw:
            return resp.status, dict(resp.headers), payload
        return resp.status, dict(resp.headers), json.loads(payload or b"{}")
    except urllib.error.HTTPError as e:
        payload = e.read()
        if raw:
            return e.code, dict(e.headers), payload
        try:
            return e.code, dict(e.headers), json.loads(payload)
        except json.JSONDecodeError:
            return e.code, dict(e.headers), {"error": payload.decode(errors="replace")}


def multipart(filename: str, content: bytes, retained: bool = False,
              sha256: str | None = None):
    boundary = "----e2e" + uuid.uuid4().hex
    lines = []
    def field(name, value):
        lines.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                     f"name=\"{name}\"\r\n\r\n{value}\r\n".encode())
    field("size", str(len(content)))
    field("retained", "1" if retained else "0")
    if sha256:
        field("sha256", sha256)
    lines.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode()
    )
    lines.append(content)
    lines.append(f"\r\n--{boundary}--\r\n".encode())
    return b"".join(lines), {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def upload(name: str, content: bytes, retained: bool = False) -> dict:
    body, headers = multipart(name, content, retained)
    status, _, data = req("POST", "/api/recordings", body, headers)
    assert status == 201, f"upload failed {status}: {data}"
    return data


def upload_raw(name: str, content: bytes, retained: bool = False,
               timeout: float = 120) -> tuple:
    """上传但允许失败，返回 (status, body dict)，用于竞态场景断言 409。

    req() 在响应可解析时已返回 dict，不可解析时返回 {"error": 原文}。
    """
    body, headers = multipart(name, content, retained)
    status, _, data = req("POST", "/api/recordings", body, headers,
                          timeout=timeout)
    return status, data if isinstance(data, dict) else {"error": str(data)}


def upload_gate(action: str) -> dict:
    status, _, data = req("POST", "/api/test/upload-gate", {"action": action})
    assert status == 200, f"upload-gate {action} failed {status}: {data}"
    return data


def wait_upload_gate(timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, _, st = req("GET", "/api/test/upload-gate")
        if st.get("reached", 0) >= 1:
            return
        time.sleep(0.05)
    raise AssertionError("上传闸门未被触发：上传没有停在登记前")


def partition_data_files(mount_path: str) -> set:
    """分区磁盘上所有数据文件（recordings/staging/moved）。"""
    found = set()
    for sub in ("recordings", "staging", "moved"):
        root = os.path.join(mount_path, sub)
        for walk_root, _dirs, files in os.walk(root):
            for name in files:
                found.add(os.path.join(walk_root, name))
    return found


def overview() -> dict:
    _, _, ov = req("GET", "/api/overview")
    return ov


def wait_state(pid: str, state: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ov = overview()
        for p in ov["partitions"]:
            if p["id"] == pid:
                if p["state"] == state:
                    return p
        time.sleep(0.5)
    raise AssertionError(f"partition {pid} did not reach state {state}: "
                         f"{overview()['partitions']}")


def part(ov, pid):
    return next(p for p in ov["partitions"] if p["id"] == pid)


def reset_world():
    """回到干净的三分区初始态：需要全新数据目录启动服务。"""
    ov = overview()
    states = sorted(p["state"] for p in ov["partitions"])
    check("初始分区数为 3", len(ov["partitions"]) == 3)
    check("初始恰好一个 active 分区",
          sum(1 for p in ov["partitions"] if p["state"] == "active") == 1,
          str(states))


def test_basic_flow():
    print("\n=== 场景1: 上传/下载/标记/Range/容量 ===")
    payload = bytes((i * 7 + 3) % 256 for i in range(200_000))
    digest = hashlib.sha256(payload).hexdigest()
    rec = upload("clip-a.bin", payload)
    check("上传后 sha256 正确", rec["sha256"] == digest)
    check("上传后落在 active 分区", rec["current_partition"] == "p1",
          rec["current_partition"])

    status, headers, body = req("GET", f"/api/recordings/{rec['id']}/download",
                                raw=True)
    check("下载状态 200 且字节一致", status == 200 and body == payload,
          f"status={status} len={len(body)}")
    check("下载响应带分区溯源头", headers.get("X-Source-Partition") == "p1")

    status, headers, body = req(
        "GET", f"/api/recordings/{rec['id']}/download",
        headers={"Range": "bytes=100-199"}, raw=True)
    check("Range 请求返回 206 且内容正确",
          status == 206 and body == payload[100:200]
          and headers.get("Content-Range", "").startswith("bytes 100-199/"),
          f"status={status} cr={headers.get('Content-Range')}")

    status, _, data = req("POST", f"/api/recordings/{rec['id']}/retained",
                          {"retained": True})
    check("标记留存成功", status == 200 and data["retained"] == 1)

    # 容量限制：上传一个超过 64MiB 的文件（服务端默认容量）
    ov = overview()
    cap = part(ov, "p1")["capacity"]
    big = b"\x00" * (cap + 1024)
    body, headers = multipart("too-big.bin", big)
    status, _, data = req("POST", "/api/recordings", body, headers)
    check("超容量上传被拒绝且无残留", status in (409, 413),
          f"status={status} resp={data}")
    ov = overview()
    check("拒绝后分区已用量未增加",
          part(ov, "p1")["used"] == 200_000, f"used={part(ov,'p1')['used']}")
    return rec


def test_recycle_during_download():
    print("\n=== 场景2: 封区回收 + 下载中回收（核心） ===")
    # p1 上准备：1 个留存片段 + 1 个未留存片段（较大，供慢速下载）
    keep = upload("keep.bin", b"KEEP-" * 50_000, retained=True)   # 300KB
    drop = upload("drop.bin", os.urandom(8 * 1024 * 1024), retained=False)  # 8MB
    ov = overview()
    check("回收前两个录像均在 p1",
          keep["current_partition"] == "p1" and drop["current_partition"] == "p1")

    # 先发起一个慢速下载（未留存片段），制造“已开始的下载”
    import threading
    dl_result = {}
    def slow_download():
        try:
            status, headers, body = req(
                "GET",
                f"/api/recordings/{drop['id']}/download?slow=0.3",
                raw=True, timeout=120)
            dl_result["status"] = status
            dl_result["ok"] = (len(body) == 8 * 1024 * 1024)
            dl_result["token"] = headers.get("X-Download-Token")
            dl_result["partition"] = headers.get("X-Source-Partition")
        except Exception as exc:
            dl_result["error"] = repr(exc)
    t = threading.Thread(target=slow_download)
    t.start()
    # 等连接建立并登记为活动下载（轮询，避免时序抖动）
    deadline = time.time() + 10
    while time.time() < deadline:
        if any(d["recording_id"] == drop["id"]
               for d in overview()["active_downloads"]):
            break
        time.sleep(0.2)

    # 封区：p1 -> sealed，p2 -> active
    status, _, data = req("POST", "/api/partitions/seal")
    check("封区成功并提升周转分区", status == 200
          and data["sealed"] == "p1" and data["new_active"] == "p2", str(data))
    wait_state("p2", "active")

    # 新录像可追加到搬入区 p2
    new_rec = upload("new-on-p2.bin", b"NEW-AREA")
    check("封区后新录像写入新 active(p2)", new_rec["current_partition"] == "p2")

    # 发起回收（场景1 中标记留存的 clip-a 也在 p1，会一并搬入 p2）
    status, _, data = req("POST", "/api/partitions/p1/recycle")
    check("回收接口返回 draining", status == 200 and data["status"] == "draining",
          str(data))
    ov = overview()
    p1_retained_ids = {r["id"] for r in ov["recordings"]
                       if r["source_partition"] == "p1" and r["retained"] == 1}
    check("p1 全部留存片段已搬到 p2",
          set(data.get("moved", [])) == p1_retained_ids
          and keep["id"] in p1_retained_ids,
          f"moved={data.get('moved')} expected={p1_retained_ids}")

    ov = overview()
    p1 = part(ov, "p1")
    check("p1 进入 draining 且慢速下载仍在途",
          p1["state"] == "draining" and p1["active_downloads"] >= 1,
          f"state={p1['state']} dl={p1['active_downloads']}")

    # 原链接（同一录像 ID）现在从新位置 p2 取录像
    status, headers, body = req(
        "GET", f"/api/recordings/{keep['id']}/download", raw=True)
    check("留存片段原链接已切换到新位置取流",
          status == 200 and headers.get("X-Source-Partition") == "p2"
          and body == b"KEEP-" * 50_000,
          f"status={status} src={headers.get('X-Source-Partition')}")
    ov = overview()
    keep_now = next(r for r in ov["recordings"] if r["id"] == keep["id"])
    check("元数据 current_partition 已切换为 p2",
          keep_now["current_partition"] == "p2" and keep_now["state"] == "moved")

    # 未留存片段停止接受新下载（在途的那个慢速下载不受影响）
    status, _, data = req(
        "GET", f"/api/recordings/{drop['id']}/download", raw=True)
    check("未留存片段拒绝新下载(409)", status == 409, str(data))

    # draining 期间手动触发排空：应因在途下载而等待，数据不被清空
    status, _, data = req("POST", "/api/partitions/p1/drain")
    check("在途下载未结束时不清空", status == 200
          and data["action"] == "waiting" and data["active_downloads"] == 1,
          str(data))
    ov = overview()
    check("等待期间 p1 上录像文件仍在（used>0）", part(ov, "p1")["used"] > 0)

    # 等慢速下载读完原字节
    t.join(timeout=60)
    check("慢速下载读完原字节且内容完整",
          dl_result.get("ok") is True and dl_result.get("status") == 200
          and dl_result.get("partition") == "p1", str(dl_result))

    # 后台线程应自动把 p1 排空到 idle；再手动触发兜底
    p1_after = wait_state("p1", "idle", timeout=25)
    check("下载结束后 p1 自动清空并回到 idle 周转池",
          p1_after["state"] == "idle" and p1_after["used"] == 0
          and p1_after["active_downloads"] == 0, str(p1_after))

    # 再封一次区，验证 idle 分区可继续参与周转（轮转顺序按接口返回判断）
    status, _, data = req("POST", "/api/partitions/seal")
    check("第二次封区成功并提升新的周转分区",
          status == 200 and data["sealed"] == "p2"
          and data["new_active"] in ("p1", "p3"), str(data))
    rec_next = upload("on-next.bin", b"NEXT")
    check("新提升的周转分区可正常承接新录像",
          rec_next["current_partition"] == data["new_active"],
          f"{rec_next['current_partition']} != {data['new_active']}")


def test_move_failure():
    print("\n=== 场景3: 搬移写入失败 + 清理周转区后重试 ===")
    # 当前状态：p2=sealed（上一场景结尾），p3=active，p1=idle
    wait_state("p2", "sealed")
    # p2 上已有上一场景搬入的留存片段（source=p1, current=p2, retained=1）
    ov = overview()
    p2_retained = [r for r in ov["recordings"]
                   if r["current_partition"] == "p2" and r["retained"] == 1]
    check("p2 上存在待搬移的留存片段", len(p2_retained) >= 1,
          f"{[r['id'] for r in p2_retained]}")

    # 注入故障：写入 2000 字节后失败
    status, _, data = req("POST", "/api/test/fail-move",
                          {"after_bytes": 2000})
    check("故障注入成功", status == 200 and data["move_fail_after"] == 2000,
          str(data))

    status, _, data = req("POST", "/api/partitions/p2/recycle")
    check("搬移失败时接口明确返回 failed",
          status == 200 and data["status"] == "failed"
          and data["failed"][0]["error"], str(data))
    failed_rid = data["failed"][0]["rid"]

    ov = overview()
    p2 = part(ov, "p2")
    check("失败后 p2 仍为 recycling 且记录错误",
          p2["state"] == "recycling" and bool(p2["last_error"]))
    rec_failed = next(r for r in ov["recordings"] if r["id"] == failed_rid)
    check("失败片段归属仍在原分区 p2（原录像可用）",
          rec_failed["current_partition"] == "p2")
    status, headers, body = req(
        "GET", f"/api/recordings/{failed_rid}/download", raw=True)
    check("失败后原录像仍可正常下载（原目录照常可用）",
          status == 200 and len(body) == rec_failed["size"],
          f"status={status}")

    # 故障触发时回收流程会即时删除本次的 .part；先手工在隔离 staging
    # 目录里放一个残留，验证 cleanup 接口确实能按源分区扫出并清理
    ov = overview()
    active_now = next(p for p in ov["partitions"] if p["state"] == "active")
    import os as _os
    stale_dir = _os.path.join(active_now["mount_path"], "staging", "p2")
    _os.makedirs(stale_dir, exist_ok=True)
    with open(_os.path.join(stale_dir, "rec_stale.part"), "wb") as f:
        f.write(b"stale-half-written")
    status, _, data = req("POST", "/api/partitions/p2/cleanup")
    check("清理周转区成功（删掉残留 .part，系统回到可重试状态）",
          status == 200 and data["removed_parts"] == 1, str(data))
    check("清理后 staging 隔离目录已移除",
          not _os.path.exists(stale_dir))
    req("POST", "/api/test/reset-fail")
    status, _, data = req("POST", "/api/partitions/p2/recycle")
    check("重试回收成功进入 draining",
          status == 200 and data["status"] == "draining", str(data))
    check("重试包含此前失败的片段", failed_rid in data["moved"],
          str(data["moved"]))

    # p2 上未留存片段：回收后拒绝新下载（409）；若已被清空则表现为文件缺失(409)
    ov = overview()
    unmarked_p2 = [r for r in ov["recordings"]
                   if r["source_partition"] == "p2" and not r["retained"]]
    if unmarked_p2:
        status2, _, d2 = req(
            "GET", f"/api/recordings/{unmarked_p2[0]['id']}/download", raw=True)
        check("未留存片段在回收后不再接受新下载", status2 == 409, str(d2))

    # 无在途下载，应很快排空到 idle
    p2_after = wait_state("p2", "idle", timeout=25)
    check("p2 排空回 idle，周转链闭环",
          p2_after["state"] == "idle" and p2_after["used"] == 0, str(p2_after))


def _arm_gate_and_start(filename: str, content: bytes, result: dict):
    """让上传停在“写完文件、尚未登记”处，返回线程。"""
    upload_gate("arm")
    def run():
        result["status"], result["body"] = upload_raw(filename, content)
    t = threading.Thread(target=run)
    t.start()
    wait_upload_gate()
    return t


def _ensure_idle_spare():
    """确保至少有一个 idle 分区可供封区轮转。"""
    ov = overview()
    if any(p["state"] == "idle" for p in ov["partitions"]):
        return
    active = next(p for p in ov["partitions"] if p["state"] == "active")
    status, _, data = req("POST", "/api/partitions/seal")
    if status == 200:
        req("POST", f"/api/partitions/{data['sealed']}/recycle")
        try:
            wait_state(data["sealed"], "idle", timeout=25)
        except AssertionError:
            pass


def test_upload_seal_race():
    print("\n=== 场景4: 封区 vs 在途上传竞态（上传已选旧写入区、登记前封区）===")
    _ensure_idle_spare()
    ov = overview()
    old_active = next(p for p in ov["partitions"] if p["state"] == "active")
    old_pid = old_active["id"]
    before_ids = {r["id"] for r in ov["recordings"]}
    old_used = part(ov, old_pid)["used"]

    result = {}
    t = _arm_gate_and_start("race-victim.bin", b"R" * 300_000, result)
    paused = partition_data_files(old_active["mount_path"])
    check("上传已暂停在登记前且文件已落盘（尚未登记）",
          len(paused) >= 1, str(paused))

    # 封区 + 立即回收：两者都必须不用等上传结束
    status, _, sealed = req("POST", "/api/partitions/seal")
    check("封区无需等待上传结束即可完成",
          status == 200 and sealed["sealed"] == old_pid, str(sealed))
    status, _, recycled = req("POST", f"/api/partitions/{old_pid}/recycle")
    check("旧区回收同样无需等待上传结束",
          status == 200 and recycled["status"] == "draining", str(recycled))
    wait_state(old_pid, "idle", timeout=25)

    # 暂停期间新上传照常写入新写入区
    new_active_id = sealed["new_active"]
    new_used_before = part(overview(), new_active_id)["used"]
    new_rec = upload("race-new.bin", b"NEW-AFTER-SEAL")
    check("封区后新上传照常写入新写入区",
          new_rec["current_partition"] == new_active_id,
          new_rec["current_partition"])

    upload_gate("release")
    t.join(timeout=60)
    check("被轮换的在途上传返回 409",
          result.get("status") == 409, f"{result.get('status')} {result.get('body')}")
    check("错误信息说明写入区已失效",
          "失效" in json.dumps(result.get("body"), ensure_ascii=False),
          str(result.get("body")))

    ov = overview()
    check("作废录像未出现在录像列表",
          {r["id"] for r in ov["recordings"]} - before_ids
          == {new_rec["id"]}, str([r["id"] for r in ov["recordings"]]))
    new_used_after = part(ov, new_active_id)["used"]
    # 新写入区在此前场景已有录像，这里只比较“本场景新增”的部分
    check("作废录像不占容量（旧区已清空、新写入区只多了这次正常上传）",
          part(ov, old_pid)["used"] == 0
          and new_used_after - new_rec["size"] == new_used_before,
          f"old={part(ov, old_pid)['used']} new={new_used_after} "
          f"before_new={new_used_before}")


def test_upload_reuse_race():
    print("\n=== 场景5: 旧写入区封区后又被重新启用，早先选中的上传仍须报错 ===")
    _ensure_idle_spare()
    ov = overview()
    cur = next(p for p in ov["partitions"] if p["state"] == "active")["id"]

    result = {}
    t = _arm_gate_and_start("race-reuse.bin", b"U" * 120_000, result)

    # 封掉它；回收清空回 idle；再封一次把它重新提升为 active
    status, _, sealed = req("POST", "/api/partitions/seal")
    assert status == 200, sealed
    status, _, recycled = req("POST", f"/api/partitions/{cur}/recycle")
    if status == 200:
        wait_state(cur, "idle", timeout=25)
    _ensure_idle_spare()
    status, _, sealed2 = req("POST", "/api/partitions/seal")
    check("同一分区编号被重新提升为写入区",
          status == 200 and sealed2["new_active"] == cur, str(sealed2))
    ov = overview()
    check("重新启用后代次已增加",
          part(ov, cur)["generation"] >= 1, str(part(ov, cur)))

    upload_gate("release")
    t.join(timeout=60)
    check("分区重新启用后，早先选中的上传仍返回 409",
          result.get("status") == 409, f"{result.get('status')} {result.get('body')}")
    check("错误信息说明写入区已轮换/失效",
          "轮换" in json.dumps(result.get("body"), ensure_ascii=False),
          str(result.get("body")))
    ov = overview()
    check("作废录像未出现在列表且不占重新启用分区的容量",
          all(r["filename"] != "race-reuse.bin" for r in ov["recordings"])
          and part(ov, cur)["used"] == 0, f"used={part(ov, cur)['used']}")
    fresh = upload("race-fresh.bin", b"FRESH-ON-REUSED")
    check("重新启用后的写入区可正常接受新上传",
          fresh["current_partition"] == cur, fresh["current_partition"])


def test_upload_seal_only_cleanup():
    print("\n=== 场景6: 只封区不回收，被拒上传的文件必须从旧区磁盘删除 ===")
    _ensure_idle_spare()
    ov = overview()
    old_pid = next(p for p in ov["partitions"] if p["state"] == "active")["id"]
    old_mount = part(ov, old_pid)["mount_path"]
    old_used_before = part(ov, old_pid)["used"]
    old_count_before = part(ov, old_pid)["recording_count"]
    before = partition_data_files(old_mount)

    result = {}
    t = _arm_gate_and_start("race-cleanup.bin", b"C" * 250_000, result)
    during = partition_data_files(old_mount)
    added = during - before
    check("暂停时旧区磁盘上有本次上传写出的文件", len(added) == 1, str(added))

    status, _, sealed = req("POST", "/api/partitions/seal")
    check("封区成功且旧区停在 sealed（暂不回收）",
          status == 200 and sealed["sealed"] == old_pid, str(sealed))

    upload_gate("release")
    t.join(timeout=60)
    check("上传被拒(409)", result.get("status") == 409,
          f"{result.get('status')} {result.get('body')}")
    after = partition_data_files(old_mount)
    check("被拒后本次上传写出的文件已从旧区磁盘删除",
          not (after & added), f"残留={after & added}")
    ov = overview()
    check("旧区(sealed)上没有该录像的元数据，也未新增容量占用",
          part(ov, old_pid)["used"] == old_used_before
          and part(ov, old_pid)["recording_count"] == old_count_before,
          str(part(ov, old_pid)))

    # 收尾：把 sealed 的旧区回收掉，别影响后续
    req("POST", f"/api/partitions/{old_pid}/recycle")
    try:
        wait_state(old_pid, "idle", timeout=25)
    except AssertionError:
        pass


def test_concurrent_uploads_during_seal():
    print("\n=== 场景7: 封区期间并发普通上传（新上传照常写当前写入区）===")
    _ensure_idle_spare()
    results = []

    def up(i):
        status, data = upload_raw(f"race-multi-{i}.bin", b"M" * 8192 + bytes([i]))
        results.append((status, data.get("current_partition"), data.get("error")))

    threads = [threading.Thread(target=up, args=(i,)) for i in range(8)]
    for th in threads:
        th.start()
    time.sleep(0.05)
    status, _, sealed = req("POST", "/api/partitions/seal")
    check("并发上传期间封区成功", status == 200, str(sealed))
    for th in threads:
        th.join(timeout=60)

    check("并发上传全部成功或明确说明写入区失效",
          len(results) == 8
          and all(s == 201 or (s == 409 and "失效" in (err or ""))
                  for s, _pid, err in results),
          str(results))
    check("成功的上传只落在封区的旧写入区或新写入区",
          all(pid in (sealed["sealed"], sealed["new_active"])
              for _s, pid, _e in results if pid),
          str(results))
    # 被拒上传不能留下元数据：列表里 race-multi-* 的条目数 == 成功数
    ov = overview()
    listed = [r for r in ov["recordings"]
              if r["filename"].startswith("race-multi-")]
    succeeded = [1 for s, _p, _e in results if s == 201]
    check("被拒上传没有在录像列表留下任何条目",
          len(listed) == len(succeeded),
          f"列表={len(listed)} 成功={len(succeeded)}")


def main():
    print(f"BASE_URL={BASE}")
    reset_world()
    test_basic_flow()
    test_recycle_during_download()
    test_move_failure()
    test_upload_seal_race()
    test_upload_reuse_race()
    test_upload_seal_only_cleanup()
    test_concurrent_uploads_during_seal()
    print("\n================ 汇总 ================")
    for tag, name, detail in RESULTS:
        print(f"{tag:4} {name}" + (f"  ({detail})" if detail and tag == "FAIL" else ""))
    print(f"通过 {PASS} / 失败 {FAIL}")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
