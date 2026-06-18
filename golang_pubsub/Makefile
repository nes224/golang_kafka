.PHONY: up down tidy run logs psql

# ยก infra (emulator + postgres)
up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

# เติม indirect deps ของ pubsub
tidy:
	go mod tidy

# รันแยก service — เปิดคนละ terminal
run-producer:
	PUBSUB_EMULATOR_HOST=localhost:8085 \
	PUBSUB_PROJECT_ID=local-project \
	go run ./cmd/producer

run-consumer:
	PUBSUB_EMULATOR_HOST=localhost:8085 \
	PUBSUB_PROJECT_ID=local-project \
	go run ./cmd/consumer

# เปิด psql ดูตาราง events / inbox
psql:
	docker compose exec postgres psql -U alphamech -d kafka_yt
