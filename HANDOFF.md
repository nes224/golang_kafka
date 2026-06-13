# HANDOFF — golang_kafka

> เอกสารส่งต่องาน เปิดอ่านอันนี้ก่อนเป็นอันแรก จะรู้ว่าโปรเจกต์อยู่ตรงไหนและทำต่อยังไง

อัปเดตล่าสุด: 2026-06-13

---

## โปรเจกต์นี้คืออะไร

Go application สาธิต Kafka pipeline ระดับ production — producer ยิง event ทุกวินาที → consumer (รองรับ rebalance) อ่านมา process แบบขนาน → เขียนลง PostgreSQL (idempotent) → commit offset แบบ sequential ต่อ partition → รันหลาย instance พร้อมกันได้ (scale ตาม k8s replicas)

เป็น **learning project** ที่เน้นเข้าใจ core mechanics ของ Kafka ลึกๆ — ยังไม่ใช่ production-ready (ดู Known Issues)

---

## 🟢 สถานะล่าสุด

อยู่ช่วงเปลี่ยนผ่านจาก single-partition → **rebalance-ready (multi-partition + scale)**

**ทำเสร็จแล้ว:**
- ✅ Core: topic/partition/offset/consumer group/commit
- ✅ Manual commit + **sequential commit** (commit เฉพาะ offset ต่อเนื่องที่เสร็จ)
- ✅ Idempotent consumer (Get ก่อน Insert + `IsDuplicateKeyErr`)
- ✅ Transaction pattern (`TxClosure` + recover/rollback)
- ✅ **Per-partition state** (`map[int32]*PartitionState`)
- ✅ State enum (Pending/Success/Error)
- ✅ **Rebalance** (cooperative-sticky + commit-before-revoke)
- ✅ `main.go` sync กับ consumer ใหม่ (เพิ่ง sync — ดู ⚠️ ข้างล่าง)

**ยังไม่ได้ทำ:** inbox/outbox/CDC, production hardening (ดู roadmap)

---

## ⚠️ สิ่งที่ต้องทำต่อทันที (Phase 0)

1. **ยืนยันว่า compile + รันผ่าน** — `main.go` เพิ่ง sync กับ consumer ใหม่ ยังไม่ได้ confirm ว่ารันได้จริง
   ```bash
   go run ./cmd
   ```
   ถ้า error → แก้ตามที่ฟ้อง (signature น่าจะตรงหมดแล้ว)

2. **ทดสอบ rebalance** — เปิด 2 terminal พร้อมกัน (`go run ./cmd` × 2) ดู log `✅ Assigned partition` / `❌ Revoking partition` ว่า partition ถูกแจก/คืนถูกต้อง

---

## 🚀 วิธีรัน (setup ตั้งแต่ศูนย์)

**Prerequisites:** Go 1.18+, Docker, `librdkafka` (macOS: `brew install librdkafka`)

```bash
# 1. Kafka (KRaft mode)
docker-compose -f kafka.yaml up -d
#    kafdrop ดู topic/message ได้ที่ http://localhost:9090

# 2. PostgreSQL — ต่อ port 5433, dbname kafka_yt, สร้างตาราง:
#    CREATE TABLE IF NOT EXISTS events (
#        event_id TEXT PRIMARY KEY, event_type TEXT, timestamp TIMESTAMPTZ);

# 3. รัน
go run ./cmd        # หรือ make app
```

config ทั้งหมดอยู่ใน `internal/shared/kafka-config.go` (topic: `local_topic_sticky1`, group: `local_cg1`, 4 partitions, cooperative-sticky)

---

## 🔴 Known Issues / Blockers

เรียงตามความเร่งด่วน (รายละเอียดเต็มใน `ARCHITECTURE.md §7`):

| # | ปัญหา | ระดับ |
|---|---|---|
| 1 | **DB password hardcode** ใน `db.go` → ต้องย้ายไป env/secret ก่อน push git | 🔴 |
| 2 | **ไม่มี graceful shutdown** — Ctrl+C ไม่ Flush producer / commit ไม่ทัน | 🔴 |
| 3 | **Replication factor = 1** — broker เดียวตาย = ข้อมูลหาย | 🔴 |
| 4 | **MsgCH block 5 วิ → drop เป็น Error** — message ถูก skip จริง (ไม่ได้ process) | 🟡 |
| 5 | **ไม่มี retry / DLQ** — Error แค่ผ่าน commit ไม่ลองใหม่ | 🟡 |
| 6 | `Event.Timespamp` สะกดผิด (ทำงานได้เพราะ db tag ถูก) | 🟢 |

---

## 📋 แผนถัดไป (สรุป — เต็มใน LEARNING_ROADMAP.md)

```
Phase 0  ✅ sync main.go → ยืนยันรันได้ + ทดสอบ rebalance   ← อยู่ตรงนี้
Phase 1  Inbox → Outbox → CDC          (correctness patterns — มีบทเรียน)
Phase 2  security/shutdown/RF/error    (Tier 1 — ก่อน ship)
Phase 3  DLQ/producer config/metrics   (Tier 2 — ก่อนรับ traffic)
Phase 4  schema registry/TLS/test      (Tier 3 — ทนทานระยะยาว)
```

---

## 📚 แผนผังเอกสาร

| ไฟล์ | เนื้อหา |
|---|---|
| **HANDOFF.md** (นี่) | จุดเริ่ม — สถานะ + วิธีรัน + ทำต่อ |
| `ARCHITECTURE.md` | ภาพรวม + mechanics + rebuild + known issues + scaling |
| `CODE_WALKTHROUGH.md` | โค้ดทีละบรรทัด เรียงตามลำดับ build |
| `MESSAGE_LIFECYCLE.md` | flow ชีวิต message (รับ→process→commit) |
| `REBALANCE.md` | rebalance เชิงลึก (per-partition, commit-before-revoke) |
| `INBOX_PATTERN.md` | กันประมวลผล event ซ้ำ (Inbox / idempotent consumer) — Phase 1.1 |
| `OUTBOX_PATTERN.md` | blueprint Transactional Outbox พร้อม implement project ใหม่ (schema+โค้ด+checklist) — Phase 1.2 |
| `CDC_DEBEZIUM.md` | ส่ง DB changes เข้า Kafka อัตโนมัติ (CDC/Debezium) + Outbox vs CDC — Phase 1.3 |
| `LEARNING_ROADMAP.md` | แผนเรียน/พัฒนาต่อ 5 phase |
| `KAFKA_GLOSSARY.md` | คำศัพท์ทั้งหมด 9 หมวด |

แนะนำลำดับอ่านสำหรับคนใหม่: HANDOFF → ARCHITECTURE → CODE_WALKTHROUGH → (REBALANCE / MESSAGE_LIFECYCLE ตามต้องการ)

---

## โครงสร้างโค้ด (quick map)

```
cmd/main.go                          ประกอบ + รัน (RunConsumer + produce + handle)
internal/shared/kafka-config.go      config กลาง
internal/shared/types.go             Message + NewMessage (unmarshal)
internal/producer/producer.go        ยิง event
internal/consumer/consumer.go        อ่าน + rebalance (assign/revoke)
internal/consumer/parition-state.go  PartitionState + commit loop (ต่อ partition)
internal/repo/db.go                  เชื่อม DB + util
internal/repo/event-repo.go          Event + CRUD + TxClosure
internal/repo/repo-err.go            ตรวจ duplicate key
```
