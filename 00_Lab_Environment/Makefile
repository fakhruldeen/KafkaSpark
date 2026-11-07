# Lab commands. Run from the repository root.
.PHONY: env up down reset check topics topics-create reset-orders logs ksql connect-source connect-sink connect-status test

env:           ## create Python virtual environment and install requirements
	@python3 -m venv .venv
	@.venv/bin/pip install --upgrade pip
	@.venv/bin/pip install -r requirements.txt
	@echo "Virtual environment created. Activate with: source .venv/bin/activate"

up:            ## start the whole stack and wait for health checks
	docker compose up -d --wait

down:          ## stop containers, keep data
	docker compose down

reset:         ## stop containers and DELETE all lab data (topics, schemas, offsets, connectors)
	docker compose down -v

check:         ## probe every service; prints PASS or FAIL per service
	@bash tools/check_stack.sh

topics-create: ## create every lab topic (RF 3, min.insync.replicas 2)
	@bash tools/create_topics.sh

reset-orders: ## empty orders.events.v1 and its DLT between labs (keeps other topics)
	@bash tools/reset_orders.sh

topics:        ## list topics on the cluster
	docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list

logs:          ## follow logs of one service: make logs SVC=kafka1
	docker compose logs -f $(SVC)

ksql:          ## open the ksqlDB command-line client (paste statements from python/s07_ksql.sql)
	docker compose exec ksqldb-server ksql http://localhost:8088

connect-source: ## register the file source connector (S06)
	curl -fsS -X POST -H "Content-Type: application/json" --data @connectors/file_source.json http://localhost:8083/connectors

connect-sink:  ## register the file sink connector (S06)
	curl -fsS -X POST -H "Content-Type: application/json" --data @connectors/file_sink.json http://localhost:8083/connectors

connect-status: ## show connector and task state (RUNNING, FAILED)
	curl -fsS http://localhost:8083/connectors?expand=status

test:          ## run the offline unit tests (no Docker needed)
	python3 -m pytest -q tests
