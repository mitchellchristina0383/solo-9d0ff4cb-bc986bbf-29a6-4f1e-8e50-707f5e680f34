# -*- coding: utf-8 -*-
"""全局配置：全部可通过环境变量覆盖。"""
import os


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class Config:
    # 数据根目录（entrypoint 会把三个分区镜像挂载到这里的 p1/p2/p3）
    DATA_DIR = os.environ.get("DATA_DIR", "/data")

    # 每个分区的等容量上限（字节）。使用 loop 镜像时应与镜像大小一致。
    PARTITION_CAPACITY = _int("PARTITION_CAPACITY", 64 * 1024 * 1024)  # 默认 64MiB
    PARTITION_COUNT = _int("PARTITION_COUNT", 3)

    # SQLite 元数据库（放在数据根目录之外的状态卷上）
    DB_PATH = os.environ.get("DB_PATH", os.path.join(DATA_DIR, "meta.db"))

    # 正在回收时，未标记片段是否直接拒绝新下载（True=拒绝，符合需求约定）
    BLOCK_UNMARKED_AFTER_RECYCLE = os.environ.get(
        "BLOCK_UNMARKED_AFTER_RECYCLE", "1"
    ) not in ("0", "false", "False", "")

    # draining 分区轮询清理间隔（秒）；也可随时手动调 drain 接口立即检查
    DRAIN_POLL_INTERVAL = _int("DRAIN_POLL_INTERVAL", 5)

    # 测试钩子开关（故障注入/慢速下载），生产环境保持 0
    ENABLE_TEST_HOOKS = os.environ.get("ENABLE_TEST_HOOKS", "0") in ("1", "true", "True")

    # 单个上传请求的最大字节数
    MAX_CONTENT_LENGTH = _int("MAX_CONTENT_LENGTH", PARTITION_CAPACITY)
