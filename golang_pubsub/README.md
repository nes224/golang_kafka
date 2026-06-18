# golang_pubsub — เวอร์ชัน Pub/Sub ของ golang_kafka

โปรเจกต์เดียวกับ `golang_kafka` แต่เปลี่ยน transport จาก **Kafka → Google Cloud Pub/Sub**
(รัน local ผ่าน emulator) เพื่อให้เห็นชัดว่า "เปลี่ยน transport แล้วอะไรเปลี่ยน อะไรเหมือนเดิม"

> สรุป 1 บรรทัด: Pub/Sub ack ทีละ message ที่ฝั่ง server → กลไก **offset / sequential-commit / rebalance ทั้งก้อนหายไป** แต่ at-least-once ยังเหมือนเดิม ดังนั้น **Inbox Pattern (กันซ้ำ) ยังจำเป็น**

---

## ภาพรวมสถาปัตยกรรม

แยกเป็น **2 service คนละ binary** (จำลอง microservices จริง — เหมือน HR↔Warehouse ใน ERP):

```
 Service A: cmd/producer            Pub/Sub (emulator)         Service B: cmd/consumer
 ───────────────────────                                       ──────────────────────
 producer (ticker 1s)  ──Publish──▶  Topic ──▶ Subscription  ──Receive──▶  handler
 (ไม่ต่อ DB)                                                                  │
                                                                             ▼
                                                           TxClosure { inbox + events }
                                                                             │
                                                               สำเร็จ → Ack · พลาด → Nack
```

> producer ยิง event อย่างเดียว (ไม่รู้จัก DB) · consumer รับไป process ลง Postgres
> ทั้งคู่แชร์แค่ `internal/*` packages + event contract (Envelope) — คุยกันผ่าน Pub/Sub เท่านั้น

ทุกอย่างคุยผ่าน interface กลาง `bus.Publisher` / `bus.Consumer` (playbook §10)
→ business/producer/handler **ไม่รู้จัก Pub/Sub ตรงๆ** สลับกลับไป Kafka แก้แค่ `internal/pubsub/*`

---

## เทียบ concept: Kafka ↔ Pub/Sub

| Kafka | Pub/Sub | หมายเหตุ |
|---|---|---|
| Topic | Topic | เหมือนกัน |
| **Partition** | **ไม่มี** (ใช้ ordering key แทนถ้าต้องเรียงลำดับ) | parallelism มาจากหลาย subscriber ไม่ใช่ partition |
| Consumer group | **Subscription** | subscription = "กลุ่มผู้รับ" · หลาย client บน sub เดียว Pub/Sub แบ่ง message ให้ |
| **Offset + manual commit** | **Ack / Nack ต่อ message** | ★ ต่างสุด — ไม่มี offset เลย |
| `enable.auto.commit=false` + commit loop | `msg.Ack()` หลัง process สำเร็จ | ไม่ต้องมี sequential-commit / PartitionState |
| Rebalance (assign/revoke callback) | **server-side อัตโนมัติ** | ไม่มีโค้ด rebalance ฝั่ง client |
| `acks=all` (producer durability) | `result.Get(ctx)` บล็อกรอ server ยืนยัน | ได้ durability เทียบเท่า |
| Redelivery เมื่อ crash ก่อน commit | Redelivery เมื่อไม่ ack ภายใน **AckDeadline** หรือ Nack | at-least-once เหมือนกัน |
| DLQ topic (ทำเอง) | Dead Letter Topic (config ที่ subscription) | Pub/Sub มีให้ในตัว |

### โค้ดที่ "หายไป" เทียบ golang_kafka
เพราะ Pub/Sub จัดการ ack state ให้ server-side ทั้งหมด ไฟล์/กลไกพวกนี้ **ไม่ต้องมี**:

- `internal/consumer/parition-state.go` ทั้งไฟล์ (PartitionState, `commitOffsetLoop`, `findLatestToCommit`)
- `rebalanceCB` / `assignPrntCB` / `revokePrtnCB` — commit-before-revoke
- การ track `maxReceived` / `lastCommited` / `state map[offset]`
- manual `CommitOffsets`

→ consumer เหลือแค่ `sub.Receive(...)` + `Ack/Nack` (~30 บรรทัด เทียบ ~420 บรรทัดของ Kafka)

### สิ่งที่ "ยังต้องมีเหมือนเดิม"
- **Inbox Pattern** (`internal/repo/inbox-repo.go`) — at-least-once = ซ้ำได้ → ยังต้อง dedup ที่ `event_id`
- **TxClosure** — ครอบ inbox + business ให้ atomic
- **Idempotency** — process ซ้ำต้องไม่พัง

---

## โครงสร้างโค้ด

```
cmd/producer/main.go             Service A — ยิง event อย่างเดียว (ไม่ต่อ DB)
cmd/consumer/main.go             Service B — รับ event ไป process ลง DB (inbox + events)
internal/shared/config.go        config ผ่าน env ทั้งหมด (ไม่ hardcode)
internal/shared/types.go         Envelope (event contract) + encode/decode
internal/bus/bus.go              ★ interface กลาง Publisher / Consumer (transport-agnostic)
internal/pubsub/client.go        สร้าง client + ensure topic/subscription (emulator-aware)
internal/pubsub/publisher.go     impl bus.Publisher ด้วย Pub/Sub
internal/pubsub/consumer.go      impl bus.Consumer ด้วย Pub/Sub (Receive + ack/nack)
internal/producer/producer.go    ยิง event ทุกวินาที (พึ่งแค่ bus.Publisher)
internal/handler/handler.go      business + Inbox Pattern (พึ่งแค่ repo)
internal/repo/db.go              เชื่อม DB (creds ผ่าน env)
internal/repo/event-repo.go      Event + Insert + TxClosure
internal/repo/inbox-repo.go      InboxRepo.MarkProcessed (ON CONFLICT DO NOTHING)
migrations/001_init.sql          ตาราง events + inbox
docker-compose.yml               pubsub-emulator + postgres
```

---

## วิธีรัน

ต้องมี: Go 1.23+, Docker

```bash
# 1. ยก emulator + postgres (migration รันอัตโนมัติ)
make up

# 2. เติม indirect deps ของ pubsub (จำเป็นรอบแรก)
make tidy        # = go mod tidy

# 3. รัน 2 service แยก terminal
make run-consumer   # terminal 1 — Service B (รับไปลง DB)
make run-producer   # terminal 2 — Service A (ยิง event)
```

producer จะ log `PUBLISHED ...` ทุกวินาที · consumer จะ log `INSERT ok ...` ตามมา
เปิดอีก terminal ดูข้อมูล:

```bash
make psql
# แล้วใน psql:
SELECT count(*) FROM events;
SELECT count(*) FROM inbox;     -- ควรเท่ากับ events (1 event = 1 inbox row)
```

### ทดสอบ idempotency / scale
- รัน `make run-consumer` **2 ตัวพร้อมกัน** (คนละ terminal) → Pub/Sub แบ่ง message ให้แต่ละตัว (ไม่ซ้ำ) โดยไม่มีโค้ด rebalance
- ลองกด Ctrl+C consumer ตัวหนึ่ง → message ที่ค้างจะไป process ที่อีกตัวเอง (server redeliver)
- inbox กันไม่ให้ count ใน `events` เกินจำนวน event จริง แม้ Pub/Sub จะ redeliver
- รัน `make run-producer` หลาย instance ได้เช่นกัน → ยิง event ถี่ขึ้น

### เปิด ordering (ทางเลือก)
ตั้ง `PUBSUB_ORDERING=true` → ใช้ `OrderingKey` (= EventType) รักษาลำดับ event ชนิดเดียวกัน
(แทนการรักษาลำดับด้วย partition ของ Kafka)

---

## ⚠️ หมายเหตุ / ข้อจำกัด

- **ยังไม่ได้ verify compile**: sandbox ที่สร้างโค้ดนี้ไม่มี Go + เข้า go proxy ไม่ได้ จึงยังไม่ได้รัน `go build`
  รบกวนรัน `go mod tidy && go build ./...` ฝั่งเครื่องตัวเอง ถ้า error ค่อยบอกมาแก้ได้
- ใช้ Pub/Sub client **v1** (`cloud.google.com/go/pubsub v1.45.x`) — เป็น API ที่นิยม/เสถียร
  (ถ้า tidy แล้วดึงเป็น v2 มา API จะต่างพอควร ให้ pin v1 ไว้ตาม go.mod)
- เป็น **learning scaffold** — ยังไม่ production-ready · ที่ยังขาดเทียบ checklist:
  - Dead Letter Topic (ตอนนี้ poison message = ack ทิ้ง)
  - retry/backoff policy ที่ subscription
  - Outbox ฝั่ง producer (ถ้าต้อง atomic กับ DB write — ดู `OUTBOX_PATTERN.md`)
  - observability (subscription backlog / oldest unacked age = ตัวชี้สุขภาพ แทน consumer lag ของ Kafka)
- inbox โตเรื่อยๆ → ต้องมี janitor ลบ row เก่า (มี index `idx_inbox_processed_at` รองรับแล้ว)
