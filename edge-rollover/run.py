# -*- coding: utf-8 -*-
"""开发/容器统一入口：初始化 DB、分区与后台排空线程，启动 Flask。"""
import os
import threading

from app import Config, bootstrap, create_app, drain_loop, init_db


def main() -> None:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    init_db(Config.DB_PATH)
    bootstrap()

    stop_event = threading.Event()
    t = threading.Thread(target=drain_loop, args=(stop_event,),
                         name="drain-loop", daemon=True)
    t.start()

    flask_app = create_app()
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    # threaded=True：下载长连接与回收/上传并发处理
    flask_app.run(host=host, port=port, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
