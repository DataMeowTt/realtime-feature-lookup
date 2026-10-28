#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."

STACK=${STACK:?STACK=a|b}
LABEL=${LABEL:-adhoc}
RATES=${RATES:-"1000 5000 10000 20000 50000"}
REPS=${REPS:-3}
WARMUP=${WARMUP:-30}
DURATION=${DURATION:-300}
UPDATE_RATE=${UPDATE_RATE:-10}
NUM_USERS=${NUM_USERS:-100000}
# HOT=1: mỗi lần chạy bật hot key ở 1 thời điểm ngẫu nhiên trong cửa sổ đo, giữ 1 khoảng ngẫu nhiên rồi tắt.
# Hot key = SHARE giao dịch dồn vào HOT_USERS user cố định (cùng nhóm user ở cả 2 stack).
HOT=${HOT:-0}
SHARE=${SHARE:-0.8}
HOT_USERS=${HOT_USERS:-20}
OUT=bench/results
mkdir -p "$OUT"

case "$STACK" in
  a)  TX=kafka; SRC=kafka ;;
  b)  TX=fluss; SRC=fluss ;;
  *)  echo "unknown STACK $STACK"; exit 1 ;;
esac
CONTROL=bench/control/generator.conf
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
TOOLS="env COMPOSE_IGNORE_ORPHANS=1 docker compose --project-directory infra -f infra/compose.common.yml --profile tools run --rm --no-deps tools java -cp"

for RATE in $RATES; do
  for REP in $(seq 1 "$REPS"); do
    RUN_ID="${LABEL}_${STACK}_${RATE}_r${REP}_$(date +%s)"
    echo "=== $RUN_ID (rate=$RATE warmup=${WARMUP}s duration=${DURATION}s) ==="
    # Mỗi lần chạy bắt đầu ở chế độ uniform; đổi bằng `make hot` / `make normal` khi đang chạy
    mkdir -p bench/control && echo "mode=uniform" > "$CONTROL"

    # M6: CPU/RAM từng container, lấy mẫu mỗi ~2s
    STATS="$OUT/${RUN_ID}_stats.csv"
    echo "ts,container,cpu_pct,mem_usage" > "$STATS"
    ( while true; do
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
          | grep '^rtfl-' | sed "s/^/$(date +%s),/" >> "$STATS"
      done ) &
    STATS_PID=$!

    $TOOLS bench/latency-consumer/target/latency-consumer.jar rtfl.bench.LatencyConsumer \
      --source "$SRC" --stack "$STACK" --label "$LABEL" --rate "$RATE" \
      --warmup "$WARMUP" --duration "$DURATION" --run-id "$RUN_ID" --out "$OUT" &
    CONS_PID=$!
    sleep 15   # consumer khởi động và chốt offset cuối trước khi có tx mới

    HOT_PID=""
    if [[ "$HOT" == 1 ]]; then
      # Seed theo (rate, rep) -> A và B gặp hot key ở cùng thời điểm tương đối, so sánh được với nhau.
      # Tính từ lúc bật generator: bắt đầu trong khoảng [WARMUP+5, WARMUP+5+DURATION/3],
      # kéo dài trong khoảng [DURATION/4, DURATION/2] -> luôn tắt trước khi cửa sổ đo kết thúc.
      RANDOM=$((RATE * 10 + REP))
      HOT_START=$((WARMUP + 5 + RANDOM % (DURATION / 3 + 1)))
      HOT_LEN=$((DURATION / 4 + RANDOM % (DURATION / 4 + 1)))
      HOT_FILE="$OUT/${RUN_ID}_hot.csv"
      echo "event,ts_ms" > "$HOT_FILE"
      echo "    hot key: bật sau ${HOT_START}s, giữ ${HOT_LEN}s ($HOT_USERS hot users, share=$SHARE)"
      ( sleep "$HOT_START"
        printf "mode=hot\nhot-users=%s\nhot-share=%s\n" "$HOT_USERS" "$SHARE" > "$CONTROL"
        echo "on,$(now_ms)" >> "$HOT_FILE"
        sleep "$HOT_LEN"
        echo "mode=uniform" > "$CONTROL"
        echo "off,$(now_ms)" >> "$HOT_FILE" ) &
      HOT_PID=$!
    fi

    $TOOLS generator/target/generator.jar rtfl.gen.Generator \
      --target "$TX" --rate "$RATE" --duration $((WARMUP + DURATION + 5)) \
      --users "$NUM_USERS" --update-rate "$UPDATE_RATE" --seed "$REP" --out-source "$SRC"

    if [[ -n "$HOT_PID" ]]; then
      kill "$HOT_PID" 2>/dev/null || true
      wait "$HOT_PID" 2>/dev/null || true
      echo "mode=uniform" > "$CONTROL"
    fi
    wait "$CONS_PID" || echo "consumer exited with error"
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
    sleep 10   # cho pipeline xả hết trước lần chạy sau
  done
done
echo "Kết quả: $OUT/summary.csv — chạy 'make report' để tổng hợp."
