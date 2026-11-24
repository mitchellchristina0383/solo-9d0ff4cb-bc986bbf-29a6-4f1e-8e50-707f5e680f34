#!/bin/sh
# 初始化三个等容量的本地分区镜像（loop 设备 + ext4），挂载到
# ${DATA_DIR}/p1 p2 p3。需要容器以 privileged（或带 CAP_SYS_ADMIN +
# /dev/loop*）运行。
#
# 不具备挂载能力时（如普通 docker、无 loop 设备）自动降级为普通目录，
# 容量上限仍由应用按 PARTITION_CAPACITY 记账强制执行，功能行为一致。
set -eu

: "${DATA_DIR:=/data}"
: "${PARTITION_COUNT:=3}"
: "${PARTITION_CAPACITY:=$((64 * 1024 * 1024))}"

mkdir -p "$DATA_DIR"

can_loop() {
    command -v losetup >/dev/null 2>&1 \
        && command -v mkfs.ext4 >/dev/null 2>&1 \
        && command -v mount >/dev/null 2>&1 \
        && ls /dev/loop-control >/dev/null 2>&1
}

MODE="directory"
if can_loop && [ "${USE_LOOP_IMAGES:-1}" = "1" ]; then
    MODE="loop"
fi

echo "[entrypoint] partition mode: $MODE (count=$PARTITION_COUNT, capacity=$PARTITION_CAPACITY bytes)"

i=1
while [ "$i" -le "$PARTITION_COUNT" ]; do
    pid="p$i"
    mountpoint="$DATA_DIR/$pid"
    image="$DATA_DIR/$pid.img"
    mkdir -p "$mountpoint"

    if [ "$MODE" = "loop" ]; then
        if [ ! -f "$image" ]; then
            echo "[entrypoint] creating image $image ($PARTITION_CAPACITY bytes)"
            truncate -s "$PARTITION_CAPACITY" "$image"
            mkfs.ext4 -q -F "$image"
        fi
        if ! mountpoint -q "$mountpoint"; then
            dev=$(losetup --find --show "$image")
            mount "$dev" "$mountpoint"
            echo "[entrypoint] mounted $image -> $mountpoint ($dev)"
        fi
    else
        echo "[entrypoint] $mountpoint 使用普通目录（loop 不可用或 USE_LOOP_IMAGES=0）"
    fi
    i=$((i + 1))
done

exec "$@"
