# 边缘录像卷回收台（edge-rollover）

Flask 实现的边缘录像卷回收页。录像写入**三份等容量的本地分区镜像**，其中始终
保留**一份作为周转区**；网页支持追加短录像、下载、标记留存、封区回收，并保证
在途下载读完原字节后才清空旧区。

## 1. 模型与状态机

三个分区 `p1 / p2 / p3`，容量均为 `PARTITION_CAPACITY`（默认 64 MiB）。
每个分区在容器内是一个挂载点（loop 镜像 + ext4），无特权时自动降级为普通目录，
容量由应用按元数据记账强制限制，行为一致。

```
 idle ──提升为写入区──▶ active ──封区──▶ sealed ──发起回收──▶ recycling
  ▲                                                    │
  │                                    搬移全部成功     │ 搬移失败（原数据不动，
  └──────────── 清空（在途下载归零）──── draining ◀─────┘ 清理周转区后可重试）
```

- `active`：唯一写入区，接受新录像追加；
- `sealed`：封区，停止写入，等待回收；封区同时把一个 `idle` 周转分区提升为新 `active`；
- `recycling`：留存片段正在搬入当前写入区（搬入区）；
- `draining`：搬移完成，未留存片段停止接受新下载，等待在途下载结束；
- `idle`：已清空的周转分区，留给下次周转。

## 2. 关键语义（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 追加短录像 | `POST /api/recordings`，流式落盘到 active 分区，sha256 + 回读校验，容量不足拒绝且无半截文件 |
| 下载 / 标记留存 | 下载支持 Range；标记只改元数据，任意状态可操作 |
| 封区后发起回收 | `seal` 先提升周转分区为新写入区，再对旧区 `recycle` |
| 标记片段全搬到周转区后，原链接从新位置取录像 | 逐文件 `复制→哈希校验→原子 rename→切换 current_partition`，录像 ID 不变，下载链接不变，服务端自动指向新位置 |
| 其余片段停止接受新下载 | recycling/draining 分区上未留存片段的新下载请求返回 409 |
| 新录像可追加到搬入区 | 搬入区就是新 active，上传与搬移可并发（staging 按源分区隔离） |
| 已开始的下载须读完原字节 | 下载开始即登记 token 与物理分区，流式读原路径；客户端断开登记 interrupted |
| 旧区等下载全结束才可清空 | draining 分区活动下载数 >0 时拒绝清空；后台线程轮询 + 手动接口双保险，归零后 wipe 回 idle |
| 搬移写入失败，原目录和录像照常可用 | 失败只产生/清理 `.part` 临时文件，源文件只读不删、元数据不切；分区留在 recycling |
| 清理周转区后可重试 | cleanup 清残留并回滚已提交片段的归属（删除孤儿副本），回收接口幂等，已搬好的文件自动跳过 |

## 3. 目录结构

```
edge-rollover/
├── app/
│   ├── config.py        # 环境变量配置
│   ├── db.py            # SQLite 元数据（分区/录像/下载会话/事件日志）
│   ├── storage.py       # 文件落盘、哈希校验、搬移、staging 隔离、故障注入
│   ├── service.py       # 分区状态机与全部业务规则
│   ├── web.py           # Flask 路由与流式下载
│   ├── templates/index.html
│   └── static/style.css
├── tests/e2e_test.py    # 端到端验收（36 项断言，含两个强制场景）
├── docker/entrypoint.d/10-partitions.sh  # loop 镜像初始化/目录降级
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── run.py
```

## 4. 启动

### Docker Compose（推荐，含 loop 分区镜像）

```bash
docker compose up -d --build
# 打开 http://localhost:8080
```

- 容器以 privileged 运行，entrypoint 自动创建并挂载三个 ext4 loop 镜像到
  `/data/p1 /data/p2 /data/p3`；
- 无 loop 能力的环境把 `USE_LOOP_IMAGES=0` 即降级为三个普通目录；
- 数据保存在命名卷 `rollover-data`。

### 本地直接运行（无 Docker 时）

```bash
pip install -r requirements.txt
DATA_DIR=./data DB_PATH=./data/meta.db \
PARTITION_CAPACITY=$((64*1024*1024)) ENABLE_TEST_HOOKS=1 \
python run.py
```

## 5. HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 管理页面 |
| GET | `/api/overview` | 分区/录像/下载/事件总览 |
| POST | `/api/recordings` | multipart 上传（字段 `file`，可选 `retained/size/sha256`） |
| POST | `/api/recordings/<rid>/retained` | 标记/取消留存 `{"retained": true}` |
| GET | `/api/recordings/<rid>/download` | 下载（支持 Range，响应头带 `X-Download-Token`、`X-Source-Partition`） |
| POST | `/api/partitions/seal` | 封区并提升周转分区 |
| POST | `/api/partitions/<pid>/recycle` | 发起/重试回收 |
| POST | `/api/partitions/<pid>/cleanup` | 失败后清理周转区并回滚 |
| POST | `/api/partitions/<pid>/drain` | 立即检查排空（后台也会自动轮询） |
| GET | `/api/downloads` | 下载会话流水 |
| POST | `/api/test/fail-move` / `/api/test/reset-fail` | 故障注入（仅 `ENABLE_TEST_HOOKS=1`） |

下载链接加 `?slow=<秒>` 可按分块节流，用于复现“下载中回收”。

## 6. 验收测试

```bash
# 先启动服务，然后：
BASE_URL=http://127.0.0.1:8080 python -I tests/e2e_test.py
```

覆盖：上传/容量/Range/标记、封区轮转、**下载中回收**（慢速下载未结束分区不
清空、结束后自动排空）、链接切换、未留存片段拒绝新下载、**搬移失败**（原文件
可用、清理回滚、幂等重试）、两轮周转闭环。另有多文件部分搬移失败的补充场景。

## 7. 已知限制 / 未完成项

- 本机环境无 Docker 守护进程，未实际执行 `docker compose build`；Dockerfile、
  compose 与 entrypoint 仅做了 shell 语法检查，loop 挂载路径需在有特权的
  Docker 上首跑确认。
- 使用 Flask 内置 WSGI 服务器（多线程），适合边缘单机小规模场景；高并发部署
  应换 waitress/gunicorn，多进程需改用文件锁/外部锁。
- 容量记账基于元数据 + 目录扫描，未做磁盘 inode 配额；loop 镜像本身有 ext4
  硬上限兜底，目录降级模式只有应用层限制。
- 下载不做鉴权（内网边缘工具定位）；如需公网使用要在前置代理加认证。
- SQLite 单文件元数据，未做主动备份；WAL 模式下可直接备份卷目录。
