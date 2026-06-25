# Curriculum — Go Concurrency + Kafka Microservice + gRPC

หลักสูตรไล่ตั้งแต่ basic → กลาง → สูง · แต่ละ Lesson สั้นๆ + ชี้ไฟล์ที่ลงลึก + คำถามเช็คตัวเอง
สถานะ: ✅ คุยแล้ว · ⏸️ ค้าง · ⬜ ยังไม่เริ่ม

```
🟢 BASIC   Lesson 1–5   : Go concurrency + Kafka core
🟡 กลาง    Lesson 6–10  : correctness (idempotent / commit / inbox / outbox / rebalance)
🔴 สูง     Lesson 11–13 : gRPC + เลือกสถาปัตยกรรม + production
```

> เป้าหมายใหญ่ที่สุดของทั้งหมด: **"service คุยกันยังไงให้ถูกต้องและไม่พัง"**

---

## 🟢 BASIC

### Lesson 1 — Goroutine + Concurrency ✅
**คือ:** `go func()` = สั่งทำงานพร้อมกัน · concurrency (ออกแบบ) ≠ parallelism (รันจริงพร้อมกัน)
**จำ:** main จบ = goroutine ตายหมด → ต้องบล็อก main ไว้
**เช็ค:** concurrency กับ parallelism ต่างกันยังไง?

### Lesson 2 — เครื่องมือ Concurrency ✅
**คือ:** 4 ตัวที่ใช้คุม goroutine
- **channel** — ส่งของระหว่าง goroutine (`MsgCH`)
- **mutex/RWMutex** — ล็อกกัน data ชนกัน (`c.mu`)
- **atomic** — กันเลขตัวเดียวชน (`maxReceived`)
- **context** — สั่งหยุด/timeout (`ctx.Done()`)
**จำ:** กัน race มี 3 ตระกูล (channel / mutex / atomic) · context = รีโมทสั่งหยุด
**เช็ค:** race condition คืออะไร · เลือก mutex vs atomic เมื่อไหร่?

### Lesson 3 — Kafka Core ✅
**คือ:** ศัพท์พื้นฐาน
- **Topic** = กล่องเก็บ message · **Partition** = เลนขนานใน topic
- **Offset** = บุ๊กมาร์กต่อ partition (เดินอิสระแต่ละเลน)
- **Consumer group** = ทีมผู้อ่าน · 1 partition → consumer เดียวในทีม
**จำ:** partition = เพดาน scale · offset มีความหมายแค่ใน partition เดียว
**ไฟล์:** `KAFKA_GLOSSARY.md`, `ARCHITECTURE.md §1-2`
**เช็ค:** topic / group / consumer ต่างกันยังไง · scale เกินจำนวน partition เกิดอะไร?

### Lesson 4 — Producer ✅
**คือ:** ตัวยิง event เข้า Kafka
**จำ:** Kafka producer = **async** (ยิงทิ้ง · ผลมาทีหลังทาง delivery report) · ไม่คืน error ตรงๆ · ต้อง Flush ก่อนปิด
**ไฟล์:** `internal/producer/producer.go`
**เช็ค:** ใครกำหนด partition/offset ตอนยิง? (broker)

### Lesson 5 — Consumer + Commit ✅(บางส่วน)
**คือ:** ตัวอ่าน message + บอก Kafka ว่าอ่านถึงไหน (commit offset)
**จำ:** `enable.auto.commit:false` = commit เอง → คุมได้ว่า commit ตอน process สำเร็จจริง
**ไฟล์:** `internal/consumer/consumer.go` (`consumeLoop` ✅) ⏸️ `appendMsgState` ค้างตรงนี้
**เช็ค:** ทำไมต้อง commit เอง ไม่ใช้ auto?

---

## 🟡 กลาง — Correctness Patterns

### Lesson 6 — Delivery Semantics + Idempotent ⬜
**คือ:** at-least-once = "ไม่หาย แต่ซ้ำได้" → consumer ต้องทนการ process ซ้ำ
**จำ:** กันซ้ำที่ **event_id (business key)** ไม่ใช่ offset
**ไฟล์:** `INBOX_PATTERN.md §1-3`
**เช็ค:** message ซ้ำเกิดจากอะไรได้บ้าง?

### Lesson 7 — Sequential Commit + Per-partition State ⏸️
**คือ:** process ขนาน (`go handleMsg`) → offset เสร็จไม่เรียง → commit ได้แค่ช่วงที่เสร็จต่อเนื่อง (ห้ามข้ามรู)
**จำ:** ใช้ atomic (`maxReceived`/`lastCommited`) + mutex (`state map`) + ticker + `ctx.Done()` พร้อมกัน — **ที่ concurrency ทุกตัวมาเจอกัน**
**ไฟล์:** `parition-state.go` (`commitOffsetLoop`, `findLatestToCommit`), `MESSAGE_LIFECYCLE.md`
**เช็ค:** ถ้า offset 5 เสร็จก่อน offset 3 — commit ได้ถึงไหน?

### Lesson 8 — Rebalance ⬜
**คือ:** Kafka แจก partition ใหม่เมื่อ consumer เพิ่ม/หาย
**จำ:** commit-before-revoke (commit งานที่เสร็จก่อนปล่อย partition) · cooperative-sticky (ย้ายเฉพาะที่จำเป็น)
**ไฟล์:** `REBALANCE.md`, consumer.go (`assignPrntCB`/`revokePrtnCB`)
**เช็ค:** ทำไมต้อง commit ก่อน revoke?

### Lesson 9 — Inbox Pattern (กันซ้ำขาเข้า) ✅
**คือ:** ตารางแยก `inbox` จำ event_id ที่เคย process · `INSERT ON CONFLICT DO NOTHING` ใน tx เดียวกับ business
**จำ:** atomic = dedup + business ไปด้วยกัน · ON CONFLICT กัน race ดีกว่า Get-ก่อน-Insert
**ไฟล์:** `INBOX_PATTERN.md`, `golang_pubsub/internal/repo/inbox-repo.go`
**เช็ค:** ทำไมแยกตาราง inbox ไม่ใช้ business table?

### Lesson 10 — Outbox Pattern (กันหายขาออก) ✅(concept)
**คือ:** เขียน event ลงตาราง `outbox` ใน tx เดียวกับ business → relay แยกอ่านไปยิง Kafka
**จำ:** แก้ dual-write (เขียน DB + ยิง Kafka ให้ atomic ไม่ได้ตรงๆ) · `enqueueEvent(ctx, tx, ...)` รับ tx จาก business มา
**ไฟล์:** `OUTBOX_PATTERN.md`, `Atomic.md`, `Outbox.md`
**เช็ค:** ทำไมยิง Kafka ตรงๆ จาก business ไม่ได้ · Inbox vs Outbox ต่างกันยังไง?

---

## 🔴 สูง

### Lesson 11 — gRPC (sync ถาม-ตอบ) ✅
**คือ:** RPC = เรียกฟังก์ชันข้ามเครื่องเหมือนเรียก local · gRPC = RPC ตัวจริงของ Google (Protobuf + HTTP/2 + .proto + gen stub)
**จำ:** RPC = แนวคิด · gRPC = ของจริง 1 ตัวในแนวคิดนั้น (เหมือนรถยนต์ vs Camry) · 4 ชนิด: Unary(sync 1-1) / 3 streaming · streaming ≠ async (ยังต่อสายค้าง)
**ไฟล์:** `golang_grpc/` (proto + server + client)
**เช็ค:** gRPC ต่างจาก REST ยังไง · streaming เป็น async ไหม? (ไม่ — ยัง sync)

### Lesson 12 — เลือกสถาปัตยกรรม: Sync vs Async ✅
**คือ:** litmus test — "ต้องการคำตอบเดี๋ยวนี้เพื่อไปต่อไหม?"
```
ถาม-ตอบ (ต้องการคำตอบสด)   → sync  → gRPC/REST
ฝากข้อความ (ไม่ต้องรอ)        → async → Kafka/PubSub
```
**จำ:** gRPC = ต่อสายตรง · Kafka = ฝากกล่องผ่าน broker · อย่าใช้ sync ทุกอย่าง (distributed monolith) หรือ async ทุกอย่าง (query ง่ายๆ ทรมาน)
**ไฟล์:** `HR_WAREHOUSE_COMMUNICATION.md` (auth=sync · data=async)
**เช็ค:** "ส่ง LINE แจ้งเตือน" sync หรือ async? (async)

### Lesson 13 — Production Hardening ⬜
**คือ:** ทำให้ทนทานพอขึ้นจริง
- **Tier 1:** ย้าย DB cred ออกจากโค้ด · graceful shutdown · replication factor ≥3 · error/retry
- **Tier 2:** DLQ (dead letter) · producer `acks=all`+idempotence · observability (consumer lag)
- **Tier 3:** schema registry (Avro/Protobuf) · TLS+SASL · testing · alerting
- **gRPC:** interceptor (auth/log middleware) · metadata (token) · mTLS · schema evolution
**ไฟล์:** `LEARNING_ROADMAP.md` (Phase 2-4), `OUTBOX_HARDENING_TODO.md`, `CDC_DEBEZIUM.md`
**เช็ค:** consumer lag คืออะไร ทำไมสำคัญ?

---

## แผนผังภาพใหญ่ (จำแค่นี้พอ)

```
                    service A คุยกับ service B

   ถาม-ตอบ ต้องการคำตอบสด ──► gRPC (sync)        ← golang_grpc · Lesson 11-12
   ฝากข้อความ ไม่ต้องรอ    ──► Kafka (async)      ← golang_pubsub/kafka · Lesson 3-10
                                  │
                          กันพังขา async:
                          Inbox (กันซ้ำรับ) + Outbox (กันหายส่ง)  ← Lesson 9-10

   ทุกอย่างรันบน Goroutine + คุมด้วย channel/mutex/atomic/context  ← Lesson 1-2
```

## 3 โปรเจกต์ที่มีในมือ

| โปรเจกต์ | คือ | Lesson |
|---|---|---|
| `golang_kafka` | Kafka microservice (ของเดิม กำลังไล่อ่าน) | 1-10 |
| `golang_pubsub` | async เวอร์ชัน GCP Pub/Sub | 1-10 |
| `golang_grpc` | sync เวอร์ชัน gRPC (HR service) | 11-12 |

## กลับมาต่อจากตรงไหน

ค้างที่ **Lesson 5/7** — `appendMsgState` → `commitOffsetLoop` ใน`golang_kafka` (จุดที่ concurrency ทุกตัวมาเจอกัน)
เปิด `STUDY_GUIDE.md` ถ้าอยากไล่ Kafka แบบเป็นทางการ · `LEARNING_LOG.md` ดูว่า session ที่แล้วคุยอะไร
