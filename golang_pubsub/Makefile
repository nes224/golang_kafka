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

# รันแอป (ชี้ client ไป emulator)
run:
	PUBSUB_EMULATOR_HOST=localhost:8085 \
	PUBSUB_PROJECT_ID=local-project \
	go run ./cmd

# เปิด psql ดูตาราง events / inbox
psql:
	docker compose exec postgres psql -U alphamech -d kafka_yt
