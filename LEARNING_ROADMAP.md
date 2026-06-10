# Learning Roadmap — golang_kafka

แผนที่การเรียนรู้ + พัฒนาโปรเจกต์ต่อ จาก "เข้าใจ core" → "ใช้ production จริงได้" จัดเป็น phase ตามลำดับที่ควรทำ

---

## ✅ ทำไปแล้ว (Done)

- Core mechanics — topic / partition / offset / consumer group / commit
- Delivery semantics — at-least-once, at-most-once, idempotent concept
- Manual commit (`enable.auto.commit: false`)
- **Sequential commit** — commit เฉพาะ offset ต่อเนื่องที่เสร็จครบ (ห้ามข้ามรู)
- Idempotent consumer แบบ basic — `Get` ก่อน `Insert` + duplicate key check
- Transaction pattern — `TxClosure` + recover + rollback
- **Per-partition state** — `map[int32]*PartitionState`
- **Rebalance** — cooperative-sticky + commit-before-revoke ✨ (เพิ่งจบ)

รากฐานแข็งแล้ว — ที่เหลือคือ "ทำให้ทนทาน + scale ได้จริง"

---

## 🔧 Phase 0 — ทำให้รันได้ก่อน (ทันที)

| งาน | ทำไม |
|---|---|
| **sync `main.go` กับ consumer ใหม่** | ตอนนี้ compile ไม่ผ่าน (signature เปลี่ยน, MarkAsComplete หาย, ไม่เรียก RunConsumer) — ดู REBALANCE.md ท้ายไฟล์ |
| ส่งผล Success/Error เข้า `UpdateState` จริง | ให้ enum MsgState_Error ทำงานตามออกแบบ ไม่ใช่ mark complete ทุกกรณี |

ต้องผ่าน phase นี้ก่อนถึงจะทดสอบ rebalance ได้ (เปิดหลาย instance ดู partition ย้าย)

---

## 📚 Phase 1 — Correctness Patterns (บทเรียนที่มีอยู่)

เรียงตาม dependency — ทำต่อยอดจากของที่มีได้เลย

### 1.1 Inbox Pattern / Idempotent Consumer
- **คืออะไร**: เก็บ event id ที่เคย process ลงตาราง `inbox` (หรือ unique constraint) เช็คก่อนทำ → กัน process ซ้ำตอน message ถูกส่งซ้ำ
- **ตอนนี้มีแล้วแบบย่อ**: `Get` ก่อน `Insert` + `IsDuplicateKeyErr` — บทเรียนจะทำให้เป็นทางการ (แยกตาราง inbox, จัดการ race)
- **ทำไมสำคัญ**: at-least-once การันตีแค่ "ไม่หาย" แต่ "อาจซ้ำ" → inbox คือเกราะกันซ้ำฝั่งรับ

### 1.2 Transactional Outbox Pattern
- **คืออะไร**: เขียน event ลงตาราง `outbox` **ใน transaction เดียวกับ business data** → worker แยกอ่าน outbox ยิงเข้า Kafka
- **แก้ปัญหา**: dual-write — เขียน DB + ยิง Kafka ให้ atomic ไม่ได้ตรงๆ (ที่เคยถามเรื่อง rollback) outbox ทำให้ "ได้ทั้งคู่ หรือไม่ได้ทั้งคู่"
- **คู่กับ inbox**: outbox (กัน event หายฝั่งส่ง) + inbox (กันซ้ำฝั่งรับ) = exactly-once แบบ practical ทั้ง pipeline

### 1.3 CDC / Debezium
- **คืออะไร**: Debezium อ่าน transaction log (WAL) ของ Postgres → ยิงการเปลี่ยนแปลงเข้า Kafka อัตโนมัติ โดยแอปไม่ต้องเขียนโค้ดยิงเอง
- **ทางเลือกแทน outbox**: เหมาะเวลาอยาก sync ข้อมูลข้ามระบบโดยไม่แตะ service เดิม
- **เรียนทีหลังสุด**: เป็น infra/architecture ใหญ่ ต้องเข้าใจ outbox ก่อนถึงเทียบข้อดีข้อเสียได้

---

## 🔴 Phase 2 — Production Hardening (Tier 1 blockers)

ต้องทำก่อน ship ของจริงเด็ดขาด — เรื่อง "เมื่อพังจะเอาตัวรอดยังไง"

| งาน | รายละเอียด |
|---|---|
| **ย้าย DB credential ออกจากโค้ด** | ตอนนี้ password อยู่ใน `db.go` → ใช้ env var / secret manager (อย่า push เข้า git) |
| **แก้ deadlock ใน commit loop** | `continue` ขณะถือ lock (ARCHITECTURE §7) — ย้าย Unlock ให้ครอบทุก path |
| **Graceful shutdown** | จับ SIGINT/SIGTERM → หยุด producer (Flush) + commit ค้าง + ปิด consumer สะอาด |
| **Error handling จริง** | insert พลาด → อย่า mark Success (ตอนนี้ message หายเงียบ) ส่ง Error state + retry/DLQ |
| **Replication factor ≥ 3** | ตอนนี้ broker เดียว RF=1 → broker ตาย = ข้อมูลหาย ต้อง multi-broker |

---

## 🟡 Phase 3 — Production Essential (Tier 2)

| งาน | รายละเอียด |
|---|---|
| **Dead Letter Queue (DLQ)** | message ที่ process ไม่ได้หลัง retry → ส่งไป topic DLQ ไม่ทิ้ง ไม่บล็อก |
| **Producer reliability config** | `acks=all` + `enable.idempotence=true` + retries — กัน event หาย/ซ้ำตอนส่ง |
| **Observability** | metrics: consumer lag, throughput, commit rate → Prometheus/Grafana (lag คือตัวชี้สุขภาพที่สำคัญสุด) |
| **Structured logging + tracing** | correlate event ข้าม service (trace id) |

---

## 🟢 Phase 4 — Maturity (Tier 3)

| งาน | รายละเอียด |
|---|---|
| **Schema Registry** (Avro/Protobuf) | ตอนนี้ JSON ดิบ ไม่มี versioning → field เปลี่ยนทีพังทั้งระบบ |
| **Security: TLS + SASL** | ตอนนี้ PLAINTEXT → เข้ารหัส + auth สำหรับ production |
| **Testing** | unit + integration test (มี `make test-app-race` แต่ยังไม่มี test file) |
| **Monitoring + Alerting** | alert เมื่อ lag สูง / consumer ตาย / DLQ โต |

---

## ภาพรวมลำดับ

```
Phase 0  sync main.go            ← ทันที (ไม่งั้นรันไม่ได้)
Phase 1  Inbox → Outbox → CDC    ← บทเรียนที่มี (correctness)
Phase 2  security/deadlock/shutdown/error/replication  ← ก่อน ship (Tier 1)
Phase 3  DLQ/producer config/observability             ← ก่อนรับ traffic (Tier 2)
Phase 4  schema/TLS/test/alerting                      ← ทนทานระยะยาว (Tier 3)
```

**คำแนะนำ**: ถ้าเป้าหมายคือ "เรียนรู้" → ลุย Phase 1 ตามบทเรียนเลย สนุกและได้ความรู้ลึก ถ้าเป้าหมายคือ "เอาขึ้น production" → Phase 0 + Phase 2 ต้องมาก่อน (ความปลอดภัยสำคัญกว่า feature)

> เอกสารอ้างอิง: `ARCHITECTURE.md` (ภาพรวม+scaling), `CODE_WALKTHROUGH.md` (โค้ดทีละบรรทัด), `MESSAGE_LIFECYCLE.md` (flow), `REBALANCE.md` (rebalance), `KAFKA_GLOSSARY.md` (คำศัพท์)
