DC       = docker compose --project-directory infra
COMMON   = -f infra/compose.common.yml
KAFKA    = -f infra/compose.kafka.yml
STACK_A  = $(COMMON) $(KAFKA) -f infra/compose.stack-a.yml
STACK_B  = $(COMMON) -f infra/compose.stack-b.yml
ALL      = $(COMMON) $(KAFKA) -f infra/compose.stack-a.yml -f infra/compose.stack-b.yml

# ---- Tham số thí nghiệm (ghi đè: make gen STACK=b RATE=5000) ----
STACK       ?= a
RATE        ?= 1000
DURATION    ?= 60
WARMUP      ?= 30
UPDATE_RATE ?= 10
NUM_USERS   ?= 100000
LABEL       ?= adhoc
export NUM_USERS PARTITIONS BUCKETS PARALLELISM LOOKUP_ASYNC FLUSS_BATCH_TIMEOUT FLUSS_FETCH_WAIT ASYNC_TIMEOUT_MS ASYNC_CAPACITY ORDERED

# Mỗi stack: tx ghi vào đâu / enriched_tx đọc từ đâu / feature store là gì
TX_a  = kafka
TX_b  = fluss
OUT_a  = kafka
OUT_b  = fluss
STORE_a  = redis
STORE_b  = fluss
# Hot key: SHARE giao dịch dồn vào HOT_USERS user cố định (giống nhau ở cả 2 stack)
SHARE     ?= 0.8
HOT_USERS ?= 20
CONTROL = bench/control/generator.conf

TOOLS = COMPOSE_IGNORE_ORPHANS=1 $(DC) $(COMMON) --profile tools run --rm --no-deps tools java -cp
GEN   = generator/target/generator.jar
CONS  = bench/latency-consumer/target/latency-consumer.jar
PROBE = bench/freshness-probe/target/freshness-probe.jar
TESTS = tests/target/scenario-tests.jar

.PHONY: build up-a up-b deploy-a deploy-b gen consume probe hot normal \
        test test-freshness bench report kill-tm redis-ui redis-ui-down ps down clean

build:
	mvn -q package -DskipTests

# ---- Hạ tầng ----
up-a:
	$(DC) $(STACK_A) up -d --build --wait
up-b:
	$(DC) $(STACK_B) up -d --build --wait

# ---- Deploy pipeline (trạng thái sạch, chờ feature store nạp đủ NUM_USERS) ----
deploy-a:
	infra/scripts/deploy.sh a
deploy-b:
	infra/scripts/deploy.sh b

# ---- Công cụ (chạy trong container, cùng mạng Docker) ----
gen:
	@mkdir -p bench/control && echo "mode=uniform" > $(CONTROL)
	$(TOOLS) $(GEN) rtfl.gen.Generator --target $(TX_$(STACK)) --rate $(RATE) --duration $(DURATION) \
	  --users $(NUM_USERS) --update-rate $(UPDATE_RATE) --out-source $(OUT_$(STACK))

consume:
	$(TOOLS) $(CONS) rtfl.bench.LatencyConsumer --source $(OUT_$(STACK)) --stack $(STACK) --label $(LABEL) \
	  --rate $(RATE) --warmup $(WARMUP) --duration $(DURATION)

probe:
	$(TOOLS) $(PROBE) rtfl.bench.FreshnessProbe --store $(STORE_$(STACK)) --stack $(STACK) --count 60

test:
	$(TOOLS) $(TESTS) rtfl.tests.ScenarioRunner --mode correctness --tx-target $(TX_$(STACK)) --out-source $(OUT_$(STACK))

test-freshness:
	$(TOOLS) $(TESTS) rtfl.tests.ScenarioRunner --mode freshness --stack $(STACK) \
	  --tx-target $(TX_$(STACK)) --out-source $(OUT_$(STACK))

# Đổi cách chọn user của generator ĐANG CHẠY (áp dụng sau ~1s, không cần khởi động lại)
hot:
	@mkdir -p bench/control
	@printf "mode=hot\nhot-users=%s\nhot-share=%s\n" $(HOT_USERS) $(SHARE) > $(CONTROL)
	@echo "generator -> $(HOT_USERS) hot users, share=$(SHARE)"

normal:
	@mkdir -p bench/control && echo "mode=uniform" > $(CONTROL)
	@echo "generator -> uniform"

# Chạy loạt thí nghiệm (xem bench/run_experiment.sh)
bench:
	STACK=$(STACK) bench/run_experiment.sh

report:
	$(if $(wildcard .venv/bin/python),.venv/bin/python,python3) bench/analysis/report.py bench/results

# M7: giết TaskManager rồi bật lại
kill-tm:
	$(DC) $(COMMON) kill taskmanager
	$(DC) $(COMMON) up -d --no-deps taskmanager

# RedisInsight (Stack A): http://localhost:5540 — tắt bằng make redis-ui-down
redis-ui:
	$(DC) $(STACK_A) --profile ui up -d redisinsight
	@echo "RedisInsight: http://localhost:5540 (database rtfl-redis = redis-1:6379, Redis Cluster 3 node)"

redis-ui-down:
	$(DC) $(STACK_A) --profile ui rm -sf redisinsight

ps:
	$(DC) $(ALL) ps

# Dừng mọi container, giữ volume Oracle (lần sau khởi động nhanh)
down:
	$(DC) $(ALL) --profile ui down

# Dừng và xoá cả dữ liệu Oracle (cần khi đổi NUM_USERS)
clean:
	$(DC) $(ALL) --profile ui down -v
