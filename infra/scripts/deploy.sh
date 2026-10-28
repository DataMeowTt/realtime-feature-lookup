#!/usr/bin/env bash
# Deploy pipeline lên stack đang chạy, luôn bắt đầu từ trạng thái sạch:
#   huỷ job cũ -> dựng lại topic/bảng -> chạy job -> chờ feature store nạp đủ NUM_USERS (note K6).
# Dùng: infra/scripts/deploy.sh a|b   (gọi qua make deploy-a / deploy-b)
set -euo pipefail
cd "$(dirname "$0")/../.."

STACK=${1:?usage: deploy.sh a|b}
NUM_USERS=${NUM_USERS:-100000}
PARTITIONS=${PARTITIONS:-4}
BUCKETS=${BUCKETS:-4}
PARALLELISM=${PARALLELISM:-4}
LOOKUP_ASYNC=${LOOKUP_ASYNC:-true}
FLUSS_BATCH_TIMEOUT=${FLUSS_BATCH_TIMEOUT:-5ms}
FLUSS_FETCH_WAIT=${FLUSS_FETCH_WAIT:-10ms}
ASYNC_TIMEOUT_MS=${ASYNC_TIMEOUT_MS:-500}
ASYNC_CAPACITY=${ASYNC_CAPACITY:-500}
ORDERED=${ORDERED:-false}
TX_RETENTION_MS=${TX_RETENTION_MS:-120000}

case "$STACK" in
  a)  FILES="-f infra/compose.common.yml -f infra/compose.kafka.yml -f infra/compose.stack-a.yml" ;;
  b)  FILES="-f infra/compose.common.yml -f infra/compose.stack-b.yml" ;;
  *)  echo "unknown stack $STACK"; exit 1 ;;
esac
dc() { docker compose --project-directory infra $FILES "$@"; }
log() { echo "[deploy-$STACK] $*"; }

cancel_jobs() {
  for j in $(curl -s localhost:8081/jobs/overview | python3 -c \
      "import sys,json;[print(j['jid']) for j in json.load(sys.stdin)['jobs'] if j['state'] in ('RUNNING','RESTARTING','CREATED')]"); do
    log "cancel job $j"
    curl -s -X PATCH "localhost:8081/jobs/$j?mode=cancel" >/dev/null
  done
  sleep 3
}

create_topics() {
  for t in "$@"; do
    dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$t" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
  done
}

# Topic luồng tx: xoá dữ liệu cũ hơn TX_RETENTION_MS (mặc định 2 phút), bất kể đã được consume hay chưa.
# segment.ms ngắn để segment đóng sớm (Kafka chỉ xoá segment đã đóng). cdc.users giữ mặc định:
# deploy-a đọc lại snapshot CDC từ đầu topic, xoá đi thì Redis không nạp đủ user.
set_tx_retention() {
  for t in "$@"; do
    dc exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
      --entity-type topics --entity-name "$t" \
      --add-config "retention.ms=$TX_RETENTION_MS,segment.ms=$((TX_RETENTION_MS / 2))" >/dev/null
  done
}

# Đếm số bản ghi đã ghi bởi job có tên chứa $1 (metric write-records lớn nhất trong các vertex)
job_written() {
  curl -s localhost:8081/jobs/overview | python3 -c "
import sys,json,urllib.request
jobs=[j for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING' and '$1' in j['name']]
n=0
for j in jobs:
    d=json.load(urllib.request.urlopen('http://localhost:8081/jobs/'+j['jid']))
    n=max([n]+[v['metrics']['write-records'] for v in d['vertices']])
print(n)"
}

wait_until() { # wait_until "<mô tả>" <lệnh trả số> <ngưỡng> <timeout giây>
  local desc=$1 cmd=$2 target=$3 timeout=$4 n=0 start=$SECONDS
  while (( SECONDS - start < timeout )); do
    n=$(eval "$cmd" 2>/dev/null || echo 0)
    [[ "$n" =~ ^[0-9]+$ ]] && (( n >= target )) && { log "$desc: $n (ok)"; return 0; }
    sleep 5
  done
  log "$desc: chỉ đạt $n/$target sau ${timeout}s"; return 1
}

REDIS_NODES="redis-1 redis-2 redis-3"

# Ghép 3 node Redis thành cluster (0 replica) nếu chưa có. Dùng IP vì redis-cli --cluster create cần IP;
# client vẫn nhận hostname nhờ cluster-announce-hostname trong compose.
ensure_redis_cluster() {
  if dc exec -T redis-1 redis-cli cluster info 2>/dev/null | grep -q 'cluster_state:ok'; then return; fi
  local addrs=""
  for n in $REDIS_NODES; do
    addrs="$addrs $(dc exec -T redis-1 getent hosts "$n" | awk '{print $1}'):6379"
  done
  log "create Redis Cluster:$addrs"
  dc exec -T redis-1 redis-cli --cluster create $addrs --cluster-replicas 0 --cluster-yes >/dev/null
  wait_until "Redis cluster ok" "dc exec -T redis-1 redis-cli cluster info | grep -c 'cluster_state:ok'" 1 60
}

redis_dbsize() { # tổng số key trên mọi node
  local total=0 n
  for n in $REDIS_NODES; do
    total=$(( total + $(dc exec -T "$n" redis-cli DBSIZE | tr -d '\r') ))
  done
  echo "$total"
}

render_sql() { # nối file SQL và thay placeholder
  cat "$@" | sed -e "s/__BUCKETS__/$BUCKETS/g" \
                 -e "s/__LOOKUP_ASYNC__/$LOOKUP_ASYNC/g" \
                 -e "s/__FLUSS_BATCH_TIMEOUT__/$FLUSS_BATCH_TIMEOUT/g" \
                 -e "s/__FLUSS_FETCH_WAIT__/$FLUSS_FETCH_WAIT/g"
}

if [[ "$STACK" == a ]]; then
  for jar in stack-a/job-profile-sync/target/job-profile-sync.jar stack-a/job-enrichment/target/job-enrichment.jar; do
    [[ -f $jar ]] || { echo "missing $jar — chạy 'make build' trước"; exit 1; }
  done
  cancel_jobs
  create_topics cdc.users tx_events enriched_tx
  set_tx_retention tx_events enriched_tx

  log "register Debezium connector"
  until curl -sf localhost:8083/connectors >/dev/null; do sleep 3; done
  curl -s -o /dev/null -X POST -H 'Content-Type: application/json' \
    --data @infra/debezium/users-connector.json localhost:8083/connectors || true

  ensure_redis_cluster
  log "reset Redis + consumer group profile-sync (A1 đọc lại cdc.users từ đầu)"
  for n in $REDIS_NODES; do dc exec -T "$n" redis-cli FLUSHALL >/dev/null; done
  dc exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --delete --group profile-sync >/dev/null 2>&1 || true

  dc cp stack-a/job-profile-sync/target/job-profile-sync.jar jobmanager:/tmp/job-profile-sync.jar >/dev/null
  dc cp stack-a/job-enrichment/target/job-enrichment.jar jobmanager:/tmp/job-enrichment.jar >/dev/null

  log "submit A1 profile-sync"
  dc exec -T jobmanager flink run -d -p "$PARALLELISM" -c rtfl.a.sync.ProfileSyncJob /tmp/job-profile-sync.jar \
    --kafka kafka:9092 --redis redis://redis-1:6379 | grep -E "JobID|Exception" || true
  wait_until "Redis keys" "redis_dbsize" "$NUM_USERS" 900

  log "submit A2 enrichment (timeout=${ASYNC_TIMEOUT_MS}ms capacity=$ASYNC_CAPACITY ordered=$ORDERED)"
  dc exec -T jobmanager flink run -d -p "$PARALLELISM" -c rtfl.a.enrich.EnrichmentJob /tmp/job-enrichment.jar \
    --kafka kafka:9092 --redis redis://redis-1:6379 \
    --timeout-ms "$ASYNC_TIMEOUT_MS" --capacity "$ASYNC_CAPACITY" --ordered "$ORDERED" | grep -E "JobID|Exception" || true
else
  cancel_jobs
  ENRICH=stack-b/sql/20_enrich.sql

  TMP=$(mktemp)
  {
    cat stack-b/sql/00_catalog.sql
    echo "SET 'parallelism.default' = '$PARALLELISM';"
    for t in enriched_tx tx_events user_profile; do echo "DROP TABLE IF EXISTS fluss_catalog.fraud.$t;"; done
    render_sql stack-b/sql/01_tables.sql stack-b/sql/10_cdc_sync.sql "$ENRICH"
  } > "$TMP"
  dc cp "$TMP" jobmanager:/tmp/deploy.sql >/dev/null
  rm -f "$TMP"

  log "submit CDC + enrichment SQL (buckets=$BUCKETS lookup.async=$LOOKUP_ASYNC batch-timeout=$FLUSS_BATCH_TIMEOUT fetch-wait=$FLUSS_FETCH_WAIT)"
  OUT=$(dc exec -T jobmanager ./bin/sql-client.sh -f /tmp/deploy.sql 2>&1)
  echo "$OUT" | grep -E "Job ID|ERROR|Exception" || true
  echo "$OUT" | grep -q "ERROR" && { echo "$OUT" | tail -40; exit 1; }
  wait_until "user_profile rows" "job_written user_profile" "$NUM_USERS" 900
fi

log "done. Flink UI: http://localhost:8081"
