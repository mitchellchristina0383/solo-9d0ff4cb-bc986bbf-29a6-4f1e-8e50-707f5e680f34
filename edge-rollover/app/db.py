# -*- coding: utf-8 -*-
"""SQLite 元数据层。

单进程多线程模型：所有元数据写操作在一把可重入锁内完成，
文件搬移等长耗时 IO 在锁外执行，只在切换录像归属时短暂持锁。
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from typing import Any, Iterable, Optional

from .config import Config

_LOCK = threading.RLock()
_conn: Optional[sqlite3.Connection] = None


SCHEMA = """
CREATE TABLE IF NOT EXISTS partitions (
    id              TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL,
    state           TEXT NOT NULL,              -- active/sealed/recycling/draining/idle
    capacity        INTEGER NOT NULL,
    mount_path      TEXT NOT NULL,
    image_path      TEXT,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS recordings (
    id                 TEXT PRIMARY KEY,
    filename           TEXT NOT NULL,
    size               INTEGER NOT NULL,
    sha256             TEXT NOT NULL,
    retained           INTEGER NOT NULL DEFAULT 0,
    source_partition   TEXT NOT NULL,
    current_partition  TEXT NOT NULL,
    relpath            TEXT NOT NULL,           -- 相对当前分区挂载点的路径
    state              TEXT NOT NULL DEFAULT 'normal',  -- normal/moved
    created_at         REAL NOT NULL,
    updated_at         REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS downloads (
    token              TEXT PRIMARY KEY,
    recording_id       TEXT NOT NULL,
    partition_id       TEXT NOT NULL,          -- 本次读取实际所在的物理分区
    byte_count         INTEGER NOT NULL DEFAULT 0,
    status             TEXT NOT NULL,          -- active/finished/interrupted
    started_at         REAL NOT NULL,
    ended_at           REAL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    level       TEXT NOT NULL,
    category    TEXT NOT NULL,
    message     TEXT NOT NULL,
    extra       TEXT
);

CREATE INDEX IF NOT EXISTS idx_rec_cur ON recordings(current_partition);
CREATE INDEX IF NOT EXISTS idx_rec_src ON recordings(source_partition);
CREATE INDEX IF NOT EXISTS idx_dl_parts ON downloads(partition_id, status);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
"""


def init_db(db_path: str) -> None:
    global _conn
    _conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    _conn.row_factory = sqlite3.Row
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA foreign_keys=ON")
    with _LOCK:
        _conn.executescript(SCHEMA)
        # 进程重启后，残留的 active 下载连接已随进程死亡，标记为 interrupted
        _conn.execute(
            "UPDATE downloads SET status='interrupted', ended_at=? "
            "WHERE status='active'",
            (time.time(),),
        )
        _conn.commit()


def db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialized"
    return _conn


def lock() -> threading.RLock:
    return _LOCK


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


# ---------------- 事件日志 ----------------

def log_event(level: str, category: str, message: str, extra: Optional[dict] = None) -> None:
    import json
    with _LOCK:
        db().execute(
            "INSERT INTO events(ts, level, category, message, extra) VALUES(?,?,?,?,?)",
            (time.time(), level, category, message,
             json.dumps(extra, ensure_ascii=False) if extra else None),
        )
        db().commit()


def list_events(limit: int = 100) -> list[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()


# ---------------- 分区 ----------------

def upsert_partition(pid: str, seq: int, state: str, capacity: int,
                     mount_path: str, image_path: Optional[str]) -> None:
    now = time.time()
    with _LOCK:
        db().execute(
            "INSERT INTO partitions(id, seq, state, capacity, mount_path, image_path,"
            " created_at, updated_at) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET state=excluded.state, "
            "capacity=excluded.capacity, mount_path=excluded.mount_path, "
            "image_path=excluded.image_path, updated_at=excluded.updated_at",
            (pid, seq, state, capacity, mount_path, image_path, now, now),
        )
        db().commit()


def get_partition(pid: str) -> Optional[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM partitions WHERE id=?", (pid,)
        ).fetchone()


def list_partitions() -> list[sqlite3.Row]:
    with _LOCK:
        return db().execute("SELECT * FROM partitions ORDER BY seq").fetchall()


def find_partition(state: str) -> Optional[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM partitions WHERE state=? ORDER BY seq LIMIT 1", (state,)
        ).fetchone()


def set_partition_state(pid: str, state: str, error: Optional[str] = None) -> None:
    with _LOCK:
        db().execute(
            "UPDATE partitions SET state=?, last_error=?, updated_at=? WHERE id=?",
            (state, error, time.time(), pid),
        )
        db().commit()


def set_partition_error(pid: str, error: Optional[str]) -> None:
    with _LOCK:
        db().execute(
            "UPDATE partitions SET last_error=?, updated_at=? WHERE id=?",
            (error, time.time(), pid),
        )
        db().commit()


# ---------------- 录像 ----------------

def insert_recording(rec: dict[str, Any]) -> None:
    now = time.time()
    with _LOCK:
        db().execute(
            "INSERT INTO recordings(id, filename, size, sha256, retained, "
            "source_partition, current_partition, relpath, state, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (rec["id"], rec["filename"], rec["size"], rec["sha256"],
             int(rec.get("retained", False)), rec["source_partition"],
             rec["current_partition"], rec["relpath"],
             rec.get("state", "normal"), now, now),
        )
        db().commit()


def get_recording(rid: str) -> Optional[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM recordings WHERE id=?", (rid,)
        ).fetchone()


def list_recordings() -> list[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM recordings ORDER BY created_at DESC"
        ).fetchall()


def recordings_on_partition(pid: str, retained: Optional[bool] = None) -> list[sqlite3.Row]:
    with _LOCK:
        if retained is None:
            return db().execute(
                "SELECT * FROM recordings WHERE current_partition=? ORDER BY created_at",
                (pid,),
            ).fetchall()
        return db().execute(
            "SELECT * FROM recordings WHERE current_partition=? AND retained=? "
            "ORDER BY created_at",
            (pid, int(retained)),
        ).fetchall()


def set_retained(rid: str, retained: bool) -> None:
    with _LOCK:
        db().execute(
            "UPDATE recordings SET retained=?, updated_at=? WHERE id=?",
            (int(retained), time.time(), rid),
        )
        db().commit()


def relocate_recording(rid: str, new_partition: str, new_relpath: str) -> None:
    """把录像的当前归属切换到新分区（文件已在新位置校验完毕后调用）。"""
    with _LOCK:
        db().execute(
            "UPDATE recordings SET current_partition=?, relpath=?, state='moved', "
            "updated_at=? WHERE id=?",
            (new_partition, new_relpath, time.time(), rid),
        )
        db().commit()


def reset_recording_location(rid: str, source_partition: str, source_relpath: str) -> None:
    """清理周转区后，把录像归属回滚到原分区（原文件一直保留着）。"""
    with _LOCK:
        db().execute(
            "UPDATE recordings SET current_partition=?, relpath=?, state='normal', "
            "updated_at=? WHERE id=?",
            (source_partition, source_relpath, time.time(), rid),
        )
        db().commit()


# ---------------- 下载会话 ----------------

def open_download(recording_id: str, partition_id: str) -> str:
    token = new_id("dl")
    with _LOCK:
        db().execute(
            "INSERT INTO downloads(token, recording_id, partition_id, status, started_at) "
            "VALUES(?,?,?,'active',?)",
            (token, recording_id, partition_id, time.time()),
        )
        db().commit()
    return token


def finish_download(token: str, byte_count: int, status: str = "finished") -> None:
    with _LOCK:
        db().execute(
            "UPDATE downloads SET status=?, byte_count=?, ended_at=? WHERE token=?",
            (status, byte_count, time.time(), token),
        )
        db().commit()


def active_download_count(partition_id: str) -> int:
    with _LOCK:
        row = db().execute(
            "SELECT COUNT(*) AS c FROM downloads "
            "WHERE partition_id=? AND status='active'",
            (partition_id,),
        ).fetchone()
        return int(row["c"])


def list_active_downloads() -> list[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM downloads WHERE status='active' ORDER BY started_at"
        ).fetchall()


def list_downloads(limit: int = 50) -> list[sqlite3.Row]:
    with _LOCK:
        return db().execute(
            "SELECT * FROM downloads ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()


def partition_used_bytes(pid: str) -> int:
    with _LOCK:
        row = db().execute(
            "SELECT COALESCE(SUM(size),0) AS s FROM recordings WHERE current_partition=?",
            (pid,),
        ).fetchone()
        return int(row["s"])


def delete_recordings_on_partition(pid: str) -> int:
    """物理清空分区时同步删除仍挂在该分区上的录像元数据。

    留存片段在搬移成功后 current_partition 已切走，不受影响；
    被删掉的都是未留存（或未及搬移）的片段。
    """
    with _LOCK:
        cur = db().execute(
            "DELETE FROM recordings WHERE current_partition=?", (pid,)
        )
        db().commit()
        return cur.rowcount
