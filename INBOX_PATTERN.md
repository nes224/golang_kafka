# Inbox Pattern — กันประมวลผล event ซ้ำ (Idempotent Consumer แบบเต็ม)

บทเรียนนี้ตอบ 3 คำถามที่ผูกกันเป็นเรื่องเดียว: **message ซ้ำเกิดได้ยังไง (ปัญหา) → idempotent consumer คืออะไร (แนวคิด) → Inbox pattern ทำให้มันถูกต้องยังไง (วิธี)**

> สรุป 1 บรรทัด: at-least-once การันตีแค่ "ไม่หาย" แต่ "อาจซ้ำ" — Inbox pattern คือการแยกตาราง "บันทึกว่าเคยเห็น event นี้แล้ว" ออกจากผลลัพธ์ทาง business เพื่อกันการ process ซ้ำให้ครอบคลุมทุก side-effect ไม่ใช่แค่ตัว insert

> อ่านคู่กับ: `MESSAGE_LIFECYCLE.md` (flow รับ→process→commit), `ARCHITECTURE.md §5.2` (idempotent ปัจจุบัน), `LEARNING_ROADMAP.md §1.1` (บทนี้คือ Phase 1.1), `HR_KAFKA_WIRING.md` (ERP จริงที่ใช้ pattern นี้)

---

## สารบัญ

1. [ปัญหา — ทำไม message ถึงซ้ำ](#1-ปัญหา--ทำไม-message-ถึงซ้ำ)
2. [Idempotent Consumer คืออะไร](#2-idempotent-consumer-คืออะไร)
3. [โค้ดปัจจุบันกันซ้ำได้แค่ไหน (และรูที่ซ่อนอยู่)](#3-โค้ดปัจจุบันกันซ้ำได้แค่ไหน-และรูที่ซ่อนอยู่)
4. [Inbox Pattern — แนวคิด](#4-inbox-pattern--แนวคิด)
5. [ลงมือทำ — โค้ด Go จริงต่อยอดจาก repo นี้](#5-ลงมือทำ--โค้ด-go-จริงต่อยอดจาก-repo-นี้)
6. [Race condition — จุดที่คนพลาดบ่อยสุด](#6-race-condition--จุดที่คนพลาดบ่อยสุด)
7. [Map กลับไป ERP จริง (hr.projects → inventory)](#7-map-กลับไป-erp-จริง-hrprojects--inventory)
8. [Inbox vs Outbox — อย่าสับสน](#8-inbox-vs-outbox--อย่าสับสน)

---

## 1. ปัญหา — ทำไม message ถึงซ้ำ

Kafka + การ design แบบ "process ก่อน commit ทีหลัง" (ที่โปรเจกต์นี้ใช้) ให้การันตีแบบ **at-least-once**: message จะถูกอ่านอย่างน้อย 1 ครั้ง ไม่หาย — **แต่อาจถูกอ่านซ้ำ** เพราะ commit ไม่ใช่ atomic กับการ process

message ซ้ำเกิดจาก 4 สาเหตุหลัก:

| สาเหตุ | เกิดตอนไหน | ในโปรเจกต์นี้ |
|---|---|---|
| **Consumer crash หลัง process ก่อน commit** | process ลง DB เสร็จ แต่ยังไม่ทัน commit offset → restart → อ่านซ้ำ | เป็นไปได้สูง: commit loop ยิงทุก 10 วิ มี window 10 วิที่ process แล้วแต่ยังไม่ commit |
| **Rebalance ระหว่าง process** | partition ถูก revoke ตอน message กำลัง process ค้าง → pod ใหม่รับไปอ่านซ้ำ | `revokePrtnCB` commit ก่อนปล่อยช่วยลด แต่ message ที่ยัง Pending ยังถูกอ่านซ้ำ |
| **Producer retry** | producer ส่งสำเร็จแต่ ack หาย → ส่งซ้ำ → มี 2 message เนื้อเดียวกัน คนละ offset | เป็นไปได้: producer ยังไม่ได้ตั้ง `enable.idempotence=true` (Phase 3) |
| **Commit ล้มเหลว** | `CommitOffsets` error → offset ไม่ขยับ → รอบหน้าอ่านซ้ำ | `commitOffsetLoop` เจอ error แค่ `continue` ไม่ retry ทันที |

**ประเด็นสำคัญ:** สาเหตุ 1-2 (crash/rebalance) ให้ message **offset เดิม** ซ้ำ ส่วนสาเหตุ 3 (producer retry) ให้ **offset ใหม่แต่ event_id เดิม** — การกันซ้ำที่ดีต้องดูที่ **business key (event_id)** ไม่ใช่ offset เพราะ offset กันได้แค่บางสาเหตุ

---

## 2. Idempotent Consumer คืออะไร

**Idempotent** = ทำซ้ำกี่ครั้งผลลัพธ์เหมือนทำครั้งเดียว ไม่พัง ไม่เพี้ยน

**Idempotent consumer** = consumer ที่ออกแบบให้ process message เดิมซ้ำได้อย่างปลอดภัย — เพราะ at-least-once การันตีว่า "จะซ้ำแน่ๆ" เราเลยไม่สู้กับการซ้ำ แต่ทำให้การซ้ำไม่มีผลเสีย

มี 2 วิธีหลัก:

```
วิธี A — พึ่ง natural idempotency ของ operation เอง
  เช่น UPSERT (INSERT ... ON CONFLICT DO UPDATE) หรือ SET x = 5
  ทำซ้ำก็ได้ผลเดิม → ไม่ต้องจำว่าเคยทำ
  ✅ ง่าย  ❌ ใช้ได้เฉพาะ operation ที่ idempotent โดยธรรมชาติ
         (ส่ง email, +1 counter, เรียก API ภายนอก → ทำซ้ำพัง)

วิธี B — จำว่า "เคยเห็น event นี้แล้ว" (dedup)
  เก็บ event_id ที่เคย process → เช็คก่อนทำ → เคยเห็น = ข้าม
  ✅ ครอบคลุมทุก operation รวม non-idempotent side-effect
  ❌ ต้องมีที่เก็บ + จัดการ race → นี่คือที่มาของ Inbox Pattern
```

โปรเจกต์นี้ตอนนี้ใช้ **วิธี B แบบลูกผสม** — แต่ผูกกับ business table โดยตรง ซึ่งมีรู (ดู §3)

---

## 3. โค้ดปัจจุบันกันซ้ำได้แค่ไหน (และรูที่ซ่อนอยู่)

`saveToDB` ใน `cmd/main.go` ทำแบบนี้:

```go
_, err := repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
    event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)   // 1. เคยมี event_id นี้ไหม
    if event != nil {
        return "", nil                                      // 2. มีแล้ว → ข้าม (idempotent skip)
    }
    id, err := s.eventRepo.Insert(ctx, tx, msg.Event)       // 3. ไม่มี → insert
    ...
})
```

**กันซ้ำได้จริง** เพราะ `event_id` เป็น PRIMARY KEY ของตาราง `events` — ต่อให้ Get พลาด (race) ตัว Insert ก็จะชน unique constraint (PG code `23505`, มี `IsDuplicateKeyErr` รอจับใน `repo-err.go`)

**แต่มี 2 รูที่ต้องเข้าใจ:**

**รูที่ 1 — idempotency ผูกกับ business table**
ตอนนี้ "การจำว่าเคยเห็น event" = "การมี row ในตาราง events" มันเป็นเรื่องเดียวกันโดยบังเอิญ เพราะ business logic คือ "insert event ลงตาราง events" พอดี

ถ้าวันหนึ่ง business เปลี่ยนเป็น "อ่าน event แล้วไปส่ง LINE notify + เรียก API คิดเงิน" (ไม่ได้ insert ลง events) — การกันซ้ำ**หายทันที** เพราะไม่มีอะไรจำว่าเคยเห็น event_id นี้ จะส่ง LINE ซ้ำ คิดเงินซ้ำ

**รูที่ 2 — side-effect นอก transaction กันซ้ำไม่ได้**
สมมติ handler ทำ 2 อย่าง: insert DB + ส่ง email ถ้า insert สำเร็จ commit แต่ email ส่งไปแล้ว พอ crash ก่อน Kafka commit → รอบหน้าอ่านซ้ำ → insert เจอ duplicate (ข้าม) **แต่ email ถูกส่งไปแล้วรอบนี้อีกใบ** เพราะ email ไม่ได้อยู่ใน DB transaction

> สรุป: โค้ดปัจจุบันคือ "idempotent ที่ถูกต้องเฉพาะกรณี side-effect เดียว = insert ลง events" Inbox pattern แยก "การ dedup" ออกมาเป็นเรื่องอิสระ เพื่อให้กันซ้ำได้ก่อนแตะ side-effect ใดๆ

---

## 4. Inbox Pattern — แนวคิด

**Inbox** = ตารางแยกต่างหากที่ทำหน้าที่ "สมุดเช็คชื่อ" ของ event ที่เคย process แล้ว มี `event_id` เป็น PK

หลักการ: **ก่อนทำ business ใดๆ ให้บันทึก event_id ลง inbox ใน transaction เดียวกัน** ถ้าบันทึกไม่ได้ (ชน PK) แปลว่าเคย process แล้ว → ข้ามทั้งก้อน

```
            ┌──────────────────────────────────────────────┐
            │  BEGIN TRANSACTION                            │
            │                                               │
            │  1. INSERT INTO inbox (event_id) ──┐          │
            │                                    │ ชน PK?   │
            │           ┌────────────────────────┘          │
            │           ▼                                   │
            │      ╱──────────╲  Yes (duplicate)            │
            │     ╱ เคยเห็น?    ╲──────▶ ROLLBACK + ข้าม     │
            │     ╲             ╱        (เคย process แล้ว)  │
            │      ╲──────────╱                             │
            │           │ No (event ใหม่)                   │
            │           ▼                                   │
            │  2. ทำ business logic (insert/update/...)     │
            │  3. (side-effect ที่ idempotent ได้ใส่ตรงนี้) │
            │                                               │
            │  COMMIT  ← inbox + business เขียนพร้อมกัน      │
            └──────────────────────────────────────────────┘
                        │
                        ▼
            UpdateState(Success) → commit Kafka offset (เหมือนเดิม)
```

**ทำไมต้องอยู่ใน transaction เดียวกับ business**
ถ้าแยก tx: insert inbox สำเร็จ (commit) → business พัง → รอบหน้าอ่านซ้ำ → inbox บอก "เคยทำแล้ว" → ข้าม → **business ไม่เคยถูกทำเลย** (ข้อมูลหาย) การอยู่ tx เดียวกันทำให้ "จำว่าเคยเห็น" กับ "ทำ business" ได้ทั้งคู่หรือไม่ได้ทั้งคู่ (atomic)

**ต่างจากโค้ดปัจจุบันยังไง**
ปัจจุบันใช้ตาราง `events` (business table) เป็นทั้งสมุดเช็คชื่อและผลลัพธ์ Inbox แยกให้ชัด: ตาราง `inbox` จำอย่างเดียว ส่วน business จะ insert/update อะไรก็เรื่องของมัน → เปลี่ยน business logic ยังไงการกันซ้ำก็ไม่พัง

---

## 5. ลงมือทำ — โค้ด Go จริงต่อยอดจาก repo นี้

### 5.1 Migration — ตาราง inbox

```sql
CREATE TABLE IF NOT EXISTS inbox (
    event_id     TEXT PRIMARY KEY,           -- business key สำหรับ dedup
    consumer     TEXT NOT NULL,              -- เผื่อหลาย consumer group ใช้ inbox ร่วม
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- เผื่อ cleanup row เก่า (inbox โตเรื่อยๆ ต้องมี retention)
CREATE INDEX IF NOT EXISTS idx_inbox_processed_at ON inbox (processed_at);
```

> เพิ่ม column `consumer` เพราะถ้าหลาย consumer group อ่าน topic เดียวกัน แต่ละ group ต้อง dedup แยก (PK ควรเป็น `(consumer, event_id)` ถ้าใช้ร่วมตาราง) ตัวอย่างนี้ใช้ event_id เดี่ยวเพื่อความง่าย

### 5.2 InboxRepo — repo ใหม่ (`internal/repo/inbox-repo.go`)

```go
package repo

import (
    "context"
    "fmt"

    "github.com/jmoiron/sqlx"
)

type InboxRepo struct {
    db        *sqlx.DB
    tableName string
}

func NewInboxRepo(db *sqlx.DB) *InboxRepo {
    return &InboxRepo{db: db, tableName: "inbox"}
}

// MarkProcessed พยายามจอง event_id ลง inbox
// คืน true = event ใหม่ (จองสำเร็จ ทำ business ต่อได้)
// คืน false = เคย process แล้ว (duplicate → ข้าม)
// ทำงานใน tx ที่ส่งเข้ามา → atomic กับ business write
func (r *InboxRepo) MarkProcessed(ctx context.Context, tx *sqlx.Tx, eventID, consumer string) (bool, error) {
    q := fmt.Sprintf(
        "INSERT INTO %s (event_id, consumer) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING",
        r.tableName,
    )
    res, err := tx.ExecContext(ctx, q, eventID, consumer)
    if err != nil {
        return false, err
    }
    n, err := res.RowsAffected()
    if err != nil {
        return false, err
    }
    return n == 1, nil   // 1 = insert จริง (ใหม่), 0 = ชน → ON CONFLICT skip (ซ้ำ)
}
```

**ทำไมใช้ `ON CONFLICT DO NOTHING` แทน Get-ก่อน-Insert**
- atomic ในคำสั่งเดียว ไม่มี window ระหว่าง Get กับ Insert ให้ race (ดู §6)
- `RowsAffected()` บอกตรงๆ ว่า insert จริงไหม → ไม่ต้องไปจับ `IsDuplicateKeyErr`
- เร็วกว่า: query เดียวแทนสองรอบ

### 5.3 แก้ saveToDB ใน `cmd/main.go`

ต่อยอดจาก `Server` เดิม — เพิ่ม `inboxRepo` เข้าไป:

```go
type Server struct {
    producer  *producer.KafkaProducer
    consumer  *consumer.KafkaConsumer
    msgCH     chan *shared.Message
    eventRepo *repo.EventRepo
    inboxRepo *repo.InboxRepo   // ★ เพิ่ม
}

func (s *Server) saveToDB(ctx context.Context, msg *shared.Message) {
    _, err := repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
        // ★ STEP 1: dedup ก่อนทำอะไรทั้งสิ้น — ใน tx เดียวกับ business
        isNew, err := s.inboxRepo.MarkProcessed(ctx, tx, msg.Event.EventId, "local_cg1")
        if err != nil {
            return "", err
        }
        if !isNew {
            // เคย process แล้ว → ข้าม business ทั้งหมด (idempotent skip)
            fmt.Printf("SKIP duplicate EventID = %s, Offset = %d\n",
                msg.Event.EventId, msg.Metadata.Offset)
            return "", nil
        }

        // ★ STEP 2: event ใหม่ → ทำ business logic ตามปกติ
        //    side-effect ที่ idempotent ได้ (insert/update DB) ใส่ตรงนี้ ปลอดภัยเพราะอยู่ใน tx เดียวกับ inbox
        id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
        if err != nil {
            return "", err
        }
        fmt.Printf("INSERT SUCCESS, EventID = %s, Offset = %d\n", id, msg.Metadata.Offset)
        return id, nil
    })

    if err != nil {
        s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Error)
        return
    }
    s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)
}
```

จุดสำคัญ: **inbox insert กับ business insert อยู่ใน `TxClosure` เดียวกัน** → commit/rollback พร้อมกัน ถ้า business พัง inbox ก็ rollback → รอบหน้าอ่านซ้ำได้ (เพราะ inbox ยังไม่จำ) ถ้าสำเร็จทั้งคู่ commit พร้อมกัน → รอบหน้าซ้ำจะถูก inbox กัน

### 5.4 side-effect ที่ idempotent ไม่ได้ (email/API/notify) ทำยังไง

ของพวกนี้อยู่ใน DB transaction ไม่ได้ (rollback ไม่ได้) วิธีถูกต้องคือ **ไม่ทำใน handler โดยตรง** แต่:

```
process event → INSERT ลง inbox + INSERT ลง outbox (tx เดียว) → commit
                                         │
                                         ▼
                            relay แยก อ่าน outbox → ส่ง email/เรียก API → mark sent
```

นี่คือเหตุผลที่ Inbox (กันซ้ำขาเข้า) มักทำคู่กับ Outbox (กันหายขาออก) → ได้ exactly-once แบบ practical ทั้ง pipeline (ดู `OUTBOX_PATTERN.md` — บทถัดไป)

---

## 6. Race condition — จุดที่คนพลาดบ่อยสุด

ตัว `handleMsg` ถูกเรียกแบบ `go s.handleMsg(msg)` (goroutine ขนาน) ถ้า producer retry ส่ง event_id เดิม 2 ใบ มันอาจถูก process **พร้อมกัน** 2 goroutine

**แบบผิด (Get ก่อน Insert คนละ statement):**

```
goroutine A          goroutine B
Get(id) → ไม่เจอ
                     Get(id) → ไม่เจอ      ← ทั้งคู่เห็น "ไม่มี"
Insert(id) → ok
                     Insert(id) → ???      ← ใบที่สองชน หรือ process ซ้ำ
```

window ระหว่าง Get กับ Insert คือช่องโหว่ ในโปรเจกต์นี้รอดเพราะ PK บังคับ (ใบสองชน 23505) แต่ถ้า business ไม่มี unique constraint = ซ้ำจริง

**แบบถูก (`INSERT ... ON CONFLICT` atomic):**

```
goroutine A                      goroutine B
INSERT ON CONFLICT → RowsAffected=1   (ได้ lock row ก่อน)
                                 INSERT ON CONFLICT → RowsAffected=0  ← DB การันตี
isNew=true → ทำ business          isNew=false → ข้าม
```

DB จัดการ atomicity ให้ในคำสั่งเดียว ไม่มี window — นี่คือเหตุผลที่ §5.2 ใช้ `ON CONFLICT DO NOTHING` + `RowsAffected()` ไม่ใช่ Get-ก่อน-Insert

> เกร็ด isolation level: `TxClosure` ตั้ง `LevelReadCommitted` ซึ่งเพียงพอสำหรับ `ON CONFLICT` (unique constraint ทำงานข้าม isolation อยู่แล้ว) ถ้าใช้ Get-ก่อน-Insert ต้องดันเป็น `Serializable` ถึงจะปลอดภัย — อีกเหตุผลที่ ON CONFLICT ดีกว่า

---

## 7. Map กลับไป ERP จริง (hr.projects → inventory)

`HR_KAFKA_WIRING.md` บอกว่าฝั่ง inventory consume `hr.projects` ไป **upsert `hr_projects` projection** — นั่นคือ **idempotency วิธี A (natural)**: upsert ทำซ้ำได้ผลเดิม สำหรับ projection ล้วนๆ อันนี้พอแล้ว ไม่ต้องมี inbox

**แต่จะต้องใช้ Inbox เมื่อ inventory เริ่มทำมากกว่า upsert:**

| inventory ทำอะไร | กันซ้ำด้วย | ต้องมี inbox? |
|---|---|---|
| upsert hr_projects (projection) | UPSERT natural idempotent | ❌ ไม่ต้อง |
| upsert + ตัด stock ของโครงการ | ตัด stock = `stock -= n` ทำซ้ำพัง | ✅ ต้อง |
| upsert + ส่ง notify ทีม + สร้าง PO | notify/PO ซ้ำไม่ได้ | ✅ ต้อง (+ outbox) |

**คำแนะนำเชิงปฏิบัติสำหรับ ERP:** ตอน consumer ของ inventory เริ่มมี side-effect ที่ไม่ใช่ pure upsert ให้เพิ่มตาราง `inbox` ใน DB ของ inventory แล้วห่อ logic เหมือน §5.3 — event_id ใช้ตัวที่มากับ event envelope (ดู `events.ProjectPayload` ในงาน HR) อย่าใช้ Kafka offset เป็น dedup key เพราะ producer retry ให้ offset ใหม่แต่ payload เดิม

---

## 8. Inbox vs Outbox — อย่าสับสน

สองตัวนี้ชื่อคล้าย แก้คนละปัญหา มักใช้คู่กัน:

| | Inbox | Outbox |
|---|---|---|
| **อยู่ฝั่ง** | ผู้รับ (consumer) | ผู้ส่ง (producer) |
| **แก้ปัญหา** | process **ซ้ำ** (dedup ขาเข้า) | event **หาย** ตอนเขียน DB+Kafka (dual-write ขาออก) |
| **กลไก** | จำ event_id ที่เคยเห็น → ชน = ข้าม | เขียน event ลงตาราง outbox ใน tx เดียวกับ business → relay ยิงเข้า Kafka |
| **การันตี** | at-least-once → effectively-once ขาเข้า | ไม่หายขาออก (atomic กับ business) |
| **ในงานนี้** | บทนี้ (ยังไม่ลงโค้ดใน golang_kafka) | ลงแล้วใน ERP (`HR_KAFKA_WIRING.md`) |

```
   Service A (HR)                    Kafka                Service B (inventory)
 business + OUTBOX  ──relay──▶  hr.projects  ──consume──▶  INBOX + business
 (กัน event หาย)                                          (กัน process ซ้ำ)
 └──────────────────── exactly-once แบบ practical ทั้ง pipeline ───────────────┘
```

**ลำดับเรียนที่แนะนำ:** บทนี้ (Inbox) → `OUTBOX_PATTERN.md` (Outbox) → `CDC_DEBEZIUM.md` (CDC/Debezium) จุดพิเศษคือ Outbox ลงมือจริงใน ERP ไปก่อนแล้ว — เรียน Inbox จบเอาไปเติมฝั่ง consumer ของ inventory ได้เลย

---

## สรุป

- **ปัญหา:** at-least-once = ซ้ำแน่ๆ (crash/rebalance/producer retry/commit fail) → กันที่ business key ไม่ใช่ offset
- **Idempotent consumer:** วิธี A (natural เช่น upsert) ง่ายแต่ใช้เฉพาะ idempotent op / วิธี B (dedup) ครอบคลุมทุก side-effect
- **Inbox pattern:** แยกตาราง dedup ออกจาก business + อยู่ tx เดียวกัน → กันซ้ำก่อนแตะ side-effect ใดๆ
- **โค้ด:** ตาราง `inbox` + `InboxRepo.MarkProcessed` (`ON CONFLICT DO NOTHING` + `RowsAffected`) ห่อใน `TxClosure` เดิม
- **Race:** ใช้ `ON CONFLICT` atomic แทน Get-ก่อน-Insert เลี่ยง window
- **ERP:** inventory upsert ตอนนี้ idempotent อยู่แล้ว เพิ่ม inbox เมื่อมี side-effect ที่ไม่ใช่ pure upsert
- **คู่กับ Outbox:** Inbox กันซ้ำขาเข้า + Outbox กันหายขาออก = exactly-once ทั้ง pipeline

> บทถัดไป: Transactional Outbox (`OUTBOX_PATTERN.md`) — blueprint พร้อม implement project ใหม่ + map ไปโค้ด ERP จริง
