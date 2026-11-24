# -*- coding: utf-8 -*-
"""Flask 路由：录像管理页 + JSON API。"""
from __future__ import annotations

import os
import time

from flask import (Flask, Response, jsonify, render_template, request,
                   stream_with_context)

from . import db, service, storage
from .config import Config


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = Config.MAX_CONTENT_LENGTH
    app.url_map.strict_slashes = False

    # ---------------- 页面 ----------------
    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/overview")
    def api_overview():
        return jsonify(service.overview())

    # ---------------- 上传 ----------------
    @app.post("/api/recordings")
    def api_upload():
        f = request.files.get("file")
        if f is None:
            return jsonify({"error": "缺少 file 表单字段"}), 400
        retained = request.form.get("retained", "0") in ("1", "true", "True")
        declared_size = request.form.get("size", type=int)
        declared_sha = request.form.get("sha256") or None
        try:
            rec = service.add_recording(
                f.filename, f.stream, declared_size, declared_sha, retained
            )
        except service.ServiceError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify(rec), 201

    # ---------------- 留存标记 ----------------
    @app.post("/api/recordings/<rid>/retained")
    def api_retain(rid):
        body = request.get_json(silent=True) or {}
        retained = bool(body.get("retained", True))
        try:
            rec = service.set_retained(rid, retained)
        except service.ServiceError as exc:
            return jsonify({"error": str(exc)}), 404
        return jsonify(rec)

    # ---------------- 下载 ----------------
    def _stream_file(path: str, token: str, partition_id: str,
                     start: int = 0, end: int | None = None,
                     delay: float = 0.0):
        """分块流式输出，全程登记为该分区上的活动下载。

        客户端中途断开也会被 finally 捕获，登记为 interrupted；
        已读完的字节照常计数。
        """
        file_size = os.path.getsize(path)
        if end is None or end >= file_size:
            end = file_size - 1
        remaining = end - start + 1

        def generate():
            sent = 0
            try:
                with open(path, "rb") as fh:
                    fh.seek(start)
                    while sent < remaining:
                        chunk = fh.read(min(storage.CHUNK, remaining - sent))
                        if not chunk:
                            break
                        yield chunk
                        sent += len(chunk)
                        if delay:
                            time.sleep(delay)
            finally:
                status = "finished" if sent >= remaining else "interrupted"
                db.finish_download(token, sent, status)
        return generate()

    @app.get("/api/recordings/<rid>/download")
    def api_download(rid):
        try:
            rec, part, path = service.resolve_for_download(rid)
        except service.ServiceError as exc:
            return jsonify({"error": str(exc)}), 409

        file_size = os.path.getsize(path)
        range_header = request.headers.get("Range")
        start, end = 0, file_size - 1
        status_code = 200
        if range_header:
            try:
                unit, rng = range_header.split("=")
                if unit.strip() != "bytes":
                    raise ValueError
                s, e = rng.split("-")
                start = int(s) if s else 0
                end = int(e) if e else file_size - 1
                if start > end or start >= file_size:
                    raise ValueError
                end = min(end, file_size - 1)
                status_code = 206
            except ValueError:
                return jsonify({"error": "非法的 Range 头"}), 416

        # 在途下载登记：记录读取时的物理分区，该分区在计数归零前不会被清空
        token = db.open_download(rid, part["id"])
        delay = 0.0
        if Config.ENABLE_TEST_HOOKS:
            delay = request.args.get("slow", default=0.0, type=float)

        generator = _stream_file(path, token, part["id"], start, end, delay)
        resp = Response(
            stream_with_context(generator),
            status=status_code,
            mimetype="application/octet-stream",
        )
        safe_name = rec["filename"].replace("\\", "_").replace('"', "'")
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="{safe_name}"'
        )
        resp.headers["Accept-Ranges"] = "bytes"
        resp.headers["X-Download-Token"] = token
        resp.headers["X-Source-Partition"] = part["id"]
        if status_code == 206:
            resp.headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            resp.headers["Content-Length"] = str(end - start + 1)
        else:
            resp.headers["Content-Length"] = str(file_size)
        return resp

    # ---------------- 分区操作 ----------------
    @app.post("/api/partitions/seal")
    def api_seal():
        try:
            result = service.seal_active()
        except service.ServiceError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify(result)

    @app.post("/api/partitions/<pid>/recycle")
    def api_recycle(pid):
        try:
            result = service.start_recycle(pid)
        except service.ServiceError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify(result)

    @app.post("/api/partitions/<pid>/cleanup")
    def api_cleanup(pid):
        try:
            result = service.cleanup_turnover(pid)
        except service.ServiceError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify(result)

    @app.post("/api/partitions/<pid>/drain")
    def api_drain(pid):
        result = service.try_drain(pid)
        return jsonify(result)

    @app.get("/api/downloads")
    def api_downloads():
        return jsonify([dict(d) for d in db.list_downloads()])

    # ---------------- 测试钩子（默认关闭）----------------
    if Config.ENABLE_TEST_HOOKS:
        @app.post("/api/test/fail-move")
        def api_fail_move():
            body = request.get_json(silent=True) or {}
            after = body.get("after_bytes")
            storage.configure_failure(move_fail_after=after)
            db.log_event("warn", "test_hook",
                         f"注入搬移失败：写入 {after} 字节后报错")
            return jsonify(storage.failure_config())

        @app.post("/api/test/reset-fail")
        def api_reset_fail():
            storage.reset_failure()
            return jsonify(storage.failure_config())

    @app.errorhandler(413)
    def too_large(_):
        return jsonify({"error": "上传体积超过服务端限制"}), 413

    @app.errorhandler(500)
    def internal(exc):
        app.logger.exception("internal error: %s", exc)
        return jsonify({"error": "服务器内部错误"}), 500

    return app
