# Event-Driven Microservices — Mechanics Playbook

> **portable reference** — เขียน generic เพื่อยกไปใช้ project อื่นได้
> reference implementation: `erp_inventory_module_be` (warehouse) ↔ `erp_hr_module_be` (HR)
> related: `HR_WAREHOUSE_COMMUNICATION.md` (เคสจริง) · `erp_kafka_module/` (shared bus + contract)

---

## สารบัญ

1. [ภาพรวม — เมื่อไหร่ใช้ pattern นี้](#1-ภาพรวม)
2. [2 วิธีที่ service คุยกัน — sync vs async](#2-sync-vs-async)
3. [ปัญหาหลัก — Dual-Write](#3-dual-write)
4. [หัวใจ — Transactional Outbox Pattern](#4-outbox)
5. [Event Envelope — สัญญากลาง](#5-envelope)
6. [ฝั่ง Producer — กลไกเต็ม](#6-producer)
7. [ฝั่ง Consumer — idempotency](#7-consumer)
8. [Delivery guarantee — at-least-once](#8-delivery)
9. [Ordering — รักษาลำดับ](#9-ordering)
10. [Transport — Kafka / Pub/Sub / abstraction](#10-transport)
11. [Failure handling](#11-failure)
12. [Operational concerns](#12-ops)
13. [Schema evolution](#13-schema)
14. [Read-Projection — sync data ข้าม service](#14-projection)
15. [Checklist — เพิ่ม pattern นี้ใน project ใหม่](#15-checklist)
16. [Anti-patterns](#16-anti)

---

<a name="1-ภาพรวม"></a>
## 1. ภาพรวม — เมื่อไหร่ใช้ pattern นี้

### ใช้เมื่อ
- มีหลาย service · แต่ละตัวมี **DB ของตัวเอง** (database-per-service)
- service ต้อง **แชร์ข้อมูล/เหตุการณ์** กันโดยไม่ผูกแน่น (loose coupling)
- ยอมรับ **eventual consistency** ได้ (ข้อมูลตรงกันช้าไป 1-2 วินาทีไม่เป็นไร)

### อย่าใช้เมื่อ
- ต้องการคำตอบ **ทันที + strong consistency** (เช่น auth check, ยอดเงินคงเหลือก่อนถอน) → ใช้ **sync REST/gRPC**
- service เดียว DB เดียว → ใช้ transaction ปกติพอ ไม่ต้องมี event

### กฎทอง
> **Event = "บอกว่าเกิดอะไรขึ้นแล้ว" (past tense)** — `order.created`, `stock.received`
> ไม่ใช่ "สั่งให้ทำ" (command) — producer ไม่รู้/ไม่สน ว่าใครจะทำอะไรต่อ

---

<a name="2-sync-vs-async"></a>
## 2. 2 วิธีที่ service คุยกัน

```
┌─ แบบ SYNC (request/response) ──────────────────────────┐
│  Service A ──HTTP/gRPC──► Service B   "ขอข้อมูล X"      │
│            ◄──ตอบทันที────                              │
│  ✅ realtime · strong consistency                       │
│  ❌ coupling แน่น · B ล่ม → A ล่มตาม · latency บวกกัน    │
│  ใช้กับ: auth, validation, ข้อมูลที่ต้องสดวินาทีนั้น     │
└─────────────────────────────────────────────────────────┘

┌─ แบบ ASYNC (event/message) ────────────────────────────┐
│  Service A ──event──► [bus] ──► Service B, C, D (ใครฟังก็รับ)│
│  ✅ decoupled · B ล่มไม่กระทบ A · เพิ่ม consumer ได้ฟรี   │
│  ❌ eventual consistency · debug ยากกว่า · ต้อง idempotent│
│  ใช้กับ: data sync, notification, side-effect, audit     │
└─────────────────────────────────────────────────────────┘
```

> ระบบจริงมักใช้ **ทั้งคู่** — auth ใช้ sync · data sync ใช้ async · เลือกตามต้องการของแต่ละ flow

---

<a name="3-dual-write"></a>
## 3. ปัญหาหลัก — Dual-Write Problem

### ❌ วิธี naive (จะเจอ bug)
```go
tx.Commit()              // 1. business commit ลง DB ✅
bus.Publish(event)       // 2. ส่ง event ──► ถ้าตรงนี้ fail = event หายถาวร 💀
```

**ทำไมพัง:** DB กับ message bus เป็นคนละระบบ · มัด transaction ร่วมกัน**ไม่ได้** → ถ้า step 2 fail (bus ล่ม, network timeout, process crash) → DB commit ไปแล้วแต่ event ไม่ออก → ข้อมูลระหว่าง service ไม่ตรงกันถาวร

**สลับลำดับก็ไม่ช่วย:**
```go
bus.Publish(event)       // ส่งก่อน
tx.Commit()              // ──► ถ้า commit fail = event ออกไปแล้วแต่ DB ไม่มีจริง 💀
```

→ ปัญหานี้แก้ด้วยการ **ไม่เขียน 2 ที่พร้อมกัน** = Outbox Pattern

---

<a name="4-outbox"></a>
## 4. หัวใจ — Transactional Outbox Pattern

### แนวคิด
แทนที่จะส่ง bus ตรงๆ ใน request → **เขียน event ลงตารางใน DB เดียวกับ business (tx เดียวกัน)** แล้วมี process แยก (relay) มาเก็บไปส่งทีหลัง

```
จังหวะ 1 · WRITE (ใน business tx · atomic)
  business write + INSERT outbox  ──► commit พร้อมกัน
                                       (DB เดียวกัน → มัด tx ได้ → atomic)
จังหวะ 2 · RELAY (process แยก · poll loop)
  SELECT outbox WHERE not published  ──► publish bus  ──► mark published
```

### 4.1 ตาราง outbox
```sql
CREATE TABLE event_outbox (
  id           text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  topic        text NOT NULL,        -- ส่งไป topic ไหน
  key          text NOT NULL DEFAULT '', -- message key (ordering)
  payload      jsonb NOT NULL,       -- envelope JSON
  attempts     integer NOT NULL DEFAULT 0,  -- นับ retry
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz           -- NULL = ยังไม่ส่ง · มีค่า = ส่งแล้ว
);
CREATE INDEX idx_outbox_unpublished ON event_outbox (created_at)
  WHERE published_at IS NULL;        -- partial index · relay query เร็ว
```

### 4.2 เขียน outbox ใน tx (atomic) — `enqueueEvent`
```go
// รับ tx เข้ามา (ไม่เปิด tx ใหม่) → INSERT outbox อยู่ tx เดียวกับ caller
func enqueueEvent(ctx context.Context, tx pgx.Tx, topic, eventType, key string, payload any) error {
    env := NewEnvelope(SourceThisService, eventType, 1, payload)  // ดู §5
    b, _ := json.Marshal(env)
    _, err := tx.Exec(ctx,
        `INSERT INTO event_outbox (topic, key, payload) VALUES ($1, $2, $3::jsonb)`,
        topic, key, string(b))
    return err
}
```

### 4.3 ใช้ใน business — atomic
```go
func (r *Repo) CreateOrder(ctx context.Context, in Order) error {
    tx, _ := r.pool.Begin(ctx)
    defer tx.Rollback(ctx)              // ◄── หลุดก่อน Commit = undo หมด

    // 1. business writes
    qtx := r.q.WithTx(tx)
    id, _ := qtx.InsertOrder(ctx, ...)
    qtx.UpdateInventory(ctx, ...)

    // 2. event ลง outbox — tx เดียวกัน · ATOMIC
    if err := enqueueEvent(ctx, tx, "orders", "order.created", id,
        OrderPayload{OrderID: id, ...}); err != nil {
        return err                      // fail → rollback ทั้ง business + outbox
    }

    return tx.Commit(ctx)               // ◄── business + outbox commit พร้อมกัน
}
```
→ **business สำเร็จ = event อยู่ใน outbox แน่นอน** · business fail = ไม่มี event · ไม่มีทางหลุดครึ่งๆ

### 4.4 Relay — poll → publish → mark
```go
func (r *Relay) Run(ctx context.Context) {
    t := time.NewTicker(2 * time.Second)
    for {
        select {
        case <-ctx.Done(): return
        case <-t.C:       r.drain(ctx)
        }
    }
}

func (r *Relay) drain(ctx context.Context) {
    rows, _ := r.repo.FetchUnpublished(ctx, 100)  // WHERE published_at IS NULL ORDER BY created_at
    for _, row := range rows {
        if err := r.bus.Publish(ctx, row.Topic, row.Key, row.Payload); err != nil {
            r.repo.IncrAttempt(ctx, row.ID)
            return  // ◄── หยุด batch · รอบหน้าลองใหม่ (รักษา order)
        }
        r.repo.MarkPublished(ctx, row.ID)  // UPDATE SET published_at = now()
    }
}
```

### ได้อะไร
| สถานการณ์ | ผล |
|---|---|
| bus ล่ม | business ทำงานปกติ · event กองใน outbox · ฟื้นแล้ว relay ส่งย้อนหลังครบ |
| DB commit แต่ publish ไม่ได้ | เป็นไปไม่ได้ — event อยู่ใน outbox (commit ไปกับ business แล้ว) |
| relay crash ตอน publish | event ยัง published_at=NULL · relay ตัวใหม่ส่งต่อ |

### 4.5 วิธีส่ง outbox → bus: Polling vs CDC (Debezium)

"relay" คือตัวที่อ่าน outbox ไปส่ง bus · ทำได้ **2 วิธี** (เลือกได้ · outbox table เหมือนเดิม):

**วิธี A · Polling relay (§4.4 · ที่ playbook นี้ใช้)**
```
relay goroutine ──poll ทุก 2s──► SELECT outbox WHERE not published ──► publish bus ──► mark
```
- ง่าย · ไม่มี infra เพิ่ม (goroutine เดียว)
- latency = poll interval (เช่น 2s)

**วิธี B · CDC (Change Data Capture · Debezium)**
```
DB เขียน transaction log (WAL/binlog) อยู่แล้ว ──► Debezium tail log ──► อ่าน outbox insert ──► publish bus
```
- Debezium = CDC tool · รันเป็น Kafka Connect plugin · tail WAL → จับทุก row change อัตโนมัติ
- latency ต่ำ (ms) · ไม่ poll
- แต่ต้องเลี้ยง **Kafka Connect cluster** + เปิด logical replication + replication slot

### 4.6 Spectrum 3 แบบ (poll ↔ CDC)

```
1. Pure CDC (Debezium อ่าน business table ตรงๆ)
   ไม่ต้องเขียน app เลย · แต่ event = raw row diff (column ของ table หลุดออกมา · leaky)

2. Outbox + Debezium (Outbox Event Router)  ◄── best practice ที่ scale ใหญ่
   app เขียน outbox (event สวย ออกแบบเอง) + Debezium tail WAL อ่าน outbox (ไม่ poll · latency ต่ำ)

3. Outbox + Polling relay  ◄── playbook นี้
   app เขียน outbox (event สวย) + relay poll (ง่าย · latency = interval)
```
> CDC/Debezium = **ทางเลือกแทน relay (วิธี A→B) · ไม่ใช่แทน outbox** · outbox table เก็บไว้เหมือนเดิม

### 4.7 เลือกยังไง

| | Polling relay | CDC / Debezium |
|---|---|---|
| app code | ต้องเขียน enqueueEvent | (pure CDC ไม่ต้อง · outbox-router ยังเขียน) |
| latency | = interval (วินาที) | ms (real-time) |
| infra | goroutine เดียว | Kafka Connect cluster (หนัก) |
| ops | แทบไม่มี | รัน+monitor Connect · WAL slot · schema |
| DB | table ปกติ | logical replication + replication slot |
| ไป Pub/Sub | relay publish Pub/Sub ตรงๆ | Debezium ผูก Kafka Connect (ไม่ natural) |

**เลือก Polling เมื่อ:** volume ต่ำ-กลาง · latency วินาทียอมรับได้ · อยาก ops ง่าย · ไม่ผูก Kafka
**เลือก CDC/Debezium เมื่อ:** volume สูงมาก · ต้องการ ms latency · **แก้ app ไม่ได้** (legacy DB · ไม่มี source) → tap log ได้โดยไม่แตะ app

> เริ่มที่ Polling (ง่ายสุด) · ถ้าโตจนต้องการ ms → อัปเป็น Outbox + Debezium ทีหลัง (outbox เดิมใช้ต่อได้ · เปลี่ยนแค่ตัว ship)

---

<a name="5-envelope"></a>
## 5. Event Envelope — สัญญากลาง

ทุก event ห่อด้วย shape เดียวกัน (metadata + payload):

```go
type Envelope struct {
    ID            string          `json:"id"`             // uuid ต่อ event → idempotency key
    Type          string          `json:"type"`           // "order.created"
    Version       int             `json:"version"`        // schema version
    Source        string          `json:"source"`         // service ที่ produce
    CorrelationID string          `json:"correlation_id"` // trace 1 flow/saga (optional)
    CausationID   string          `json:"causation_id"`   // event ที่ทำให้เกิด event นี้ (optional)
    Timestamp     time.Time       `json:"timestamp"`
    Payload       json.RawMessage `json:"payload"`        // ตัวข้อมูลจริง
}
```

**ทำไมต้องมี envelope (ไม่ส่ง payload เปล่า):**
- `id` → consumer ใช้กันซ้ำ (idempotency)
- `type` → consumer route ได้โดยไม่ต้อง deserialize payload ก่อน
- `version` → รองรับ schema เปลี่ยน
- `correlation_id` → ตาม flow ข้าม service ตอน debug

**Topic vs Type:**
- **topic** = ช่อง/หมวดใหญ่ (`orders`) — consumer subscribe ระดับนี้
- **type** = เหตุการณ์ย่อย (`order.created`, `order.cancelled`) — อยู่ใน envelope · consumer filter เอง

---

<a name="6-producer"></a>
## 6. ฝั่ง Producer — กลไกเต็ม

```
┌─ Request (ใน HTTP handler / service) ──────────────────┐
│  tx := pool.Begin()                                     │
│  ├─ business writes (qtx.*)                             │
│  ├─ enqueueEvent(tx, ...)   ← INSERT outbox (atomic)    │
│  └─ tx.Commit()                                         │
│  return 200  ← ตอบ user ทันที (ไม่รอ bus)               │
└─────────────────────────────────────────────────────────┘
        │ event อยู่ใน outbox table แล้ว
        ▼
┌─ Relay goroutine (แยก · background) ───────────────────┐
│  loop ทุก 2s:                                           │
│    fetch unpublished → publish bus → mark published     │
└─────────────────────────────────────────────────────────┘
        │
        ▼  bus (Kafka / Pub/Sub)
```

**กฎ:**
- `enqueueEvent` **ต้องอยู่ใน tx เดียวกับ business** เสมอ · ห้ามเรียกหลัง commit
- request path **ไม่แตะ bus เลย** · bus เป็นเรื่องของ relay
- relay 1 ตัวต่อ service (หรือ leader-elected ถ้า multi-replica · ดู §12)

---

<a name="7-consumer"></a>
## 7. ฝั่ง Consumer — Manual Commit + DB Idempotency

at-least-once → message อาจ **หาย** (ถ้า commit ผิดจังหวะ) หรือ **ซ้ำ** (redeliver) · ต้อง 2 กลไกคู่กัน

### 7.1 ต้นตอ — "ช่องว่าง" ระหว่าง DB commit กับ offset commit
```
1. อ่าน message (offset N)
2. เขียน DB (business effect)
3. commit offset N

crash ระหว่าง 2-3:  DB commit แล้ว ✅ · offset ยังไม่ ✗
restart → อ่าน N ซ้ำ → process ซ้ำ → DUPLICATE 💀
```

### 7.2 Manual Commit — กัน message "หาย"

**❌ Auto-commit (อันตราย · default ของหลาย client):**
```
library commit offset อัตโนมัติทุก ~5s · ไม่สนว่า process เสร็จจริงไหม
poll คืน m1,m2,m3 (offset→3) → auto-commit → commit 3
เพิ่ง process m1 · crash ตอน m2 → restart resume จาก 3 → m2,m3 หายถาวร 💀
```
→ แยก "offset ที่ commit" ออกจาก "ที่ process จริง" = เสี่ยงหาย

**✅ Manual-commit (ปลอดภัย):**
```go
for {
    msg := consumer.Poll()
    if err := process(msg); err != nil {
        continue                 // ไม่ commit → message กลับมาใหม่
    }
    consumer.CommitOffset(msg)   // ◄── commit หลัง process สำเร็จเท่านั้น
}
```
→ รับประกัน **ไม่หาย** (at-least-once) · แต่ยังซ้ำได้ → ต้อง 7.3

### 7.3 DB Idempotency — กัน duplicate "ทำซ้ำ"

**วิธี A · UPSERT by natural key (state sync — ง่ายสุด):**
```go
db.Exec(`
    INSERT INTO hr_users (user_id, email, name) VALUES ($1,$2,$3)
    ON CONFLICT (user_id) DO UPDATE SET email=$2, name=$3`,
    p.UserID, p.Email, p.Name)
// insert ซ้ำ = update ทับ · ผลเหมือนเดิม
```

**วิธี B · Inbox table (side-effect ที่ทำซ้ำไม่ได้ — email, +/- ยอด, สร้าง record):**
```go
func handle(env Envelope) error {
    tx, _ := db.Begin()
    defer tx.Rollback()

    var seen bool
    tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM processed_events WHERE id=$1)`, env.ID).Scan(&seen)
    if seen { return nil }       // เคยทำแล้ว · skip

    doBusinessEffect(tx, env)    // business + dedup record · ATOMIC tx เดียว
    tx.Exec(`INSERT INTO processed_events (id) VALUES ($1)`, env.ID)
    return tx.Commit()
}
```
> = **Inbox Pattern** — กระจกสะท้อนของ outbox · ฝั่งส่งมี outbox · ฝั่งรับมี inbox

### 7.4 รวม 2 อัน + **ลำดับที่ถูก** = effectively-once

```
1. อ่าน message
2. DB transaction (ATOMIC):  เช็ค id → ถ้าเคย skip · ถ้าไม่ทำ business + insert id → COMMIT
3. commit offset / ack       ◄── หลัง DB commit เท่านั้น
```

| crash ตอน | restart แล้ว |
|---|---|
| ก่อน/ระหว่าง step 2 | tx rollback · ไม่มี effect → อ่านใหม่ · process ปกติ |
| **หลัง 2 ก่อน 3** | DB เห็น id แล้ว → **skip** → commit offset · ไม่ซ้ำ ✅ |
| หลัง 3 | เสร็จ · ไป message ถัดไป |

→ business effect เกิด **พอดี 1 ครั้ง** แม้ deliver หลายครั้ง

### 7.5 ⚠️ "effectively-once" ≠ "exactly-once delivery"
```
message อาจ DELIVER หลายครั้ง (at-least-once · เปลี่ยนไม่ได้)
แต่ PROCESS/มี effect พอดี 1 ครั้ง   ◄── นี่คือเป้าหมาย
```
manual commit (ไม่หาย) + DB idempotency (ไม่ซ้ำ) = **effectively-once** · ได้ผลเท่า exactly-once โดยไม่ต้องจ่ายความซับซ้อนของ exactly-once จริง

### 7.6 Kafka vs Pub/Sub — หลักเดียวกัน
| | Kafka | Pub/Sub |
|---|---|---|
| "ทำเสร็จ" | commit offset | ack message |
| auto (อันตราย) | `enable.auto.commit=true` | auto-ack / ack deadline |
| manual (ปลอดภัย) | `CommitMessages()` หลัง process | `msg.Ack()` หลัง process |
| process fail | ไม่ commit → redeliver | `msg.Nack()` → redeliver |

> หลักการเดียวกัน — ack/commit ทีหลัง + inbox dedup · เปลี่ยน transport ไม่ต้องแก้ logic นี้

---

<a name="8-delivery"></a>
## 8. Delivery Guarantee — at-least-once

| guarantee | ความหมาย | ใช้กับ |
|---|---|---|
| at-most-once | ส่ง ≤ 1 ครั้ง · อาจหาย | log ที่ยอมหายได้ |
| **at-least-once** | ส่ง ≥ 1 ครั้ง · อาจซ้ำ | **มาตรฐาน** (outbox + idempotent consumer) |
| exactly-once | ส่งพอดี 1 ครั้ง · ไม่หายไม่ซ้ำ | แพง/ซับซ้อน · มักไม่จำเป็น |

> **Outbox pattern = at-least-once** · จับคู่กับ **idempotent consumer** → ได้ผลเหมือน exactly-once โดยไม่ต้องจ่ายค่าความซับซ้อนของ exactly-once จริง

---

<a name="9-ordering"></a>
## 9. Ordering, Partition & Rebalancing — รักษาลำดับ + กระจายโหลด

### 9.1 ปัญหา ordering
event ของ entity เดียวกันต้องเรียงถูก เช่น `order.created` ต้องมาก่อน `order.cancelled` · ถ้าสลับ = state พัง

### 9.2 หลักการกลาง: message key = entity id
```go
bus.Publish(ctx, "orders", orderID, payload)
//                          ^^^^^^^ key = entity id (order id)
```
- event ที่ **key เดียวกัน** → เรียงตามลำดับ (รับประกัน)
- event ต่าง key → ขนานกันได้ (throughput สูง)

> ❗ **relay ต้อง publish เรียงตาม `created_at` + หยุด batch เมื่อ publish fail** (ไม่ข้าม) → ไม่งั้น order เพี้ยน (ดู §4.4 `return` on fail)

แต่ "key เดียวกันเรียงถูก" ทำงานเบื้องหลังต่างกันมากระหว่าง Kafka กับ Pub/Sub ↓

### 9.3 Kafka — Partition-based model

```
Topic "orders" แบ่งเป็น N partition (fixed · ตั้งตอนสร้าง · เพิ่มได้ ลดไม่ได้)
┌──────────────────────────────────┐
│ P0: [m1][m5][m9]   ← ordered log  │
│ P1: [m2][m6]                      │
│ P2: [m3][m7]                      │
│ P3: [m4][m8]                      │
└──────────────────────────────────┘
       │ assign (1 partition → 1 consumer ในกลุ่ม)
  ┌────┴────┐
  ▼         ▼
Consumer A  Consumer B   (consumer group)
P0,P1       P2,P3        ← "เป็นเจ้าของ" partition
```

- **key → hash → partition** · order รับประกัน **ภายใน partition**
- **parallelism เพดาน = จำนวน partition** (4 partition → มากสุด 4 consumer · ตัวที่ 5 นั่งว่าง)
- offset track ต่อ partition ต่อ consumer group

### 9.4 Rebalancing (เฉพาะ Kafka)

**Rebalancing** = ตอน consumer เข้า/ออก/crash → Kafka **เอา partition มาแจกใหม่** ทั้งกลุ่ม

```
ก่อน:  A=[P0,P1]  B=[P2,P3]
B crash ──► REBALANCE ──► A=[P0,P1,P2,P3]   (A รับช่วง partition ของ B)
```

- ระหว่าง rebalance = **"stop-the-world"** · ทุก consumer หยุด process ชั่วคราว
- consumer flap (เข้าๆ ออกๆ) → rebalance บ่อย → throughput ตก
- เหตุผลที่ต้อง rebalance: **partition ผูกกับ consumer (ownership)** → membership เปลี่ยน = ต้องแจก ownership ใหม่

### 9.5 Pub/Sub — Message-based model (ไม่มี partition · ไม่มี rebalance)

```
Topic "orders" = stream ของ message (ไม่แบ่ง partition · ซ่อน shard ไว้)
┌──────────────────────────────────┐
│ [m1][m2][m3][m4][m5][m6][m7]...   │
└──────────────────────────────────┘
       │ Subscription (broker แจก message ทีละอันให้ใครก็ได้ที่ว่าง)
  ┌────┼────┬────────┐
  ▼    ▼    ▼        ▼
 Sub A Sub B Sub C  Sub D   ← เพิ่ม/ลด อิสระ · ไม่เป็นเจ้าของอะไร
```

- **ไม่มี partition** ให้เห็น/ตั้งค่า — Pub/Sub auto-shard ภายใน · จัดการให้
- **ไม่มี ownership** — broker ถือ message แล้วแจกทีละอันให้ subscriber ที่ pull มา
- **ไม่มี rebalance "stop-the-world"** — เพิ่ม subscriber → แจก message ให้มากขึ้นทันที · ลด subscriber → message ที่ยังไม่ ack เด้งไปตัวอื่น · ไม่มีพิธีแจกใหม่
- **parallelism ยืดหยุ่น** — ไม่มีเพดาน partition · เพิ่ม subscriber เท่าไหร่ scale ตาม

> หัวใจความต่าง: **rebalance เกิดเพราะ Kafka ผูก partition กับ consumer · Pub/Sub ไม่ผูก เลยไม่ต้อง rebalance**

### 9.6 Ordering ใน Pub/Sub = Ordering Key

Pub/Sub default **ไม่รับประกัน order** (message ขนานกัน · อาจสลับ) · ถ้าต้องการ order → เปิด **ordering key**:

```go
msg := &pubsub.Message{
    Data:        payload,
    OrderingKey: orderID,   // ◄── เทียบเท่า message key ของ Kafka
}
```
- ordering key เดียวกัน → ส่งเรียง · ไป subscriber เดียว · ทีละอัน (ต้อง ack ก่อนส่งตัวถัดไป)
- ordering key ต่างกัน → ขนานได้

```
Kafka:   key → hash → 1 ใน N partition (fixed)  · order ต่อ partition
Pub/Sub: ordering key → virtual lane ต่อ key     · order ต่อ key · lane ไม่จำกัดจำนวน
```

### 9.7 เทียบ + เลือกใช้

| | Kafka | Pub/Sub |
|---|---|---|
| partition | fixed · ตั้ง+tune เอง | ไม่มี (auto-shard ซ่อน) |
| rebalancing | มี · stop-the-world | **ไม่มี** · elastic |
| parallelism เพดาน | = จำนวน partition | ไม่จำกัด |
| consumer > partition | ตัวเกินนั่งว่าง | ไม่มีปัญหานี้ |
| ordering default | ต่อ partition (ฟรี) | ไม่มี (ต้องเปิด ordering key) |
| ต้อง tune | จำนวน partition | ไม่ต้อง |
| ops | คุมเอง · monitor lag ต่อ partition | จัดการให้หมด |

**เลือก:**
- volume สูง · ต้องคุม parallelism เป๊ะ · ใช้ ecosystem → **Kafka** (ยอมจัดการ partition + rebalance)
- volume ต่ำ-กลาง · อยาก ops ง่าย · scale ยืดหยุ่น → **Pub/Sub** (ordering key แทน partition-by-key)

> code ที่ใช้ `key = aggregate id` อยู่แล้ว ย้ายไป Pub/Sub ได้ตรงๆ — map key เป็น ordering key · order ต่อ entity ยังถูก · ไม่ต้องห่วง partition/rebalance

> ⚠️ **Pub/Sub Lite** (เคยมี partition แบบ Kafka · zonal · ถูกกว่า) ถูก **deprecate** แล้ว → ยึด **standard Pub/Sub** (ไม่มี partition)

---

<a name="10-transport"></a>
## 10. Transport — Kafka / Pub/Sub / abstraction

### กฎสำคัญ: abstract transport ไว้
อย่าให้ business/outbox/consumer รู้จัก Kafka/Pub/Sub ตรงๆ · ครอบด้วย interface:

```go
// bus/publisher.go — interface กลาง
type Publisher interface {
    Publish(ctx context.Context, topic, key string, payload []byte) error
}
type Consumer interface {
    Subscribe(ctx context.Context, topic, group string, handler func(Envelope) error) error
}
```

→ สลับ transport = แก้แค่ implementation ของ interface นี้ (1-2 ไฟล์) · ที่เหลือไม่กระทบ

> ✅ **โปรเจกต์นี้ทำแล้ว (2026-06-13):** `bus.Publisher`/`bus.Consumer` เป็น interface · concrete = `KafkaPublisher`/`KafkaConsumer` + `PubSubPublisher`/`PubSubConsumer` · เลือกด้วย `EVENT_TRANSPORT=kafka|pubsub` (`bus.Configure`) · **prod เคาะ = Pub/Sub** (ดู `PUBSUB_SETUP.md`) · relay/outbox/inbox/consumer logic ไม่ต้องแตะตอนสลับ — พิสูจน์แล้วว่า abstraction คุ้ม

### เทียบ transport
| | Kafka | Pub/Sub (GCP) | Redpanda | RabbitMQ |
|---|---|---|---|---|
| ops | self-host หนัก / managed แพง | serverless · ไม่ต้องดูแล | เบากว่า Kafka | ปานกลาง |
| cost (low volume) | สูง (cluster always-on) | ~ฟรี (pay-per-use) | ปานกลาง | ปานกลาง |
| protocol | Kafka | gRPC (เฉพาะ GCP) | Kafka-compatible | AMQP |
| เหมาะกับ | volume สูง · ecosystem | internal app · GCP | Kafka แต่ประหยัด | task queue |

> **เลือกตาม:** volume ต่ำ + cloud-native → managed serverless (Pub/Sub/SQS+SNS) · volume สูง + ecosystem → Kafka

---

<a name="11-failure"></a>
## 11. Failure Handling

| failure | เกิดอะไร | จัดการ |
|---|---|---|
| **bus ล่ม** | event กองใน outbox | relay retry อัตโนมัติ · ไม่เสียข้อมูล (outbox = buffer) |
| **relay crash** | event ค้าง published_at=NULL | relay ตัวใหม่ส่งต่อจากที่ค้าง |
| **consumer crash** | offset ไม่ commit | message redeliver · idempotent กันซ้ำ |
| **poison message** | event ที่ publish ไม่ได้ถาวร | ⚠️ **block ทั้งคิว** (relay return on fail) → ต้องมี **dead-letter** |
| **duplicate** | at-least-once | idempotent consumer (§7) |

### Dead-Letter (สำคัญ · มักลืม)
poison message (เช่น payload เกิน size limit, topic ไม่มี) จะ block คิวตลอดกาล · ต้อง:
```go
if row.Attempts >= maxAttempts {
    r.repo.MarkDeadLetter(ctx, row.ID)  // ย้ายไป failed/DLQ · ข้ามไป
    continue
}
```
> Pub/Sub/SQS มี **dead-letter topic native** · Kafka self-host ต้องเขียนเอง

---

<a name="12-ops"></a>
## 12. Operational Concerns

### 12.1 Outbox cleanup (table โตไม่จำกัด)
```sql
-- janitor (cron) ลบ row ที่ส่งแล้วเกิน 7 วัน
DELETE FROM event_outbox WHERE published_at < now() - interval '7 days';
```

### 12.2 Multi-replica relay (กัน double-publish)
ถ้ารัน service หลาย instance → ทุกตัวมี relay → publish ซ้ำ
```sql
-- ใช้ SKIP LOCKED → แต่ละ relay หยิบคนละ row · ไม่ชนกัน
SELECT * FROM event_outbox WHERE published_at IS NULL
ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED;
```
หรือ **leader election** (1 instance ทำ relay) · หรือ at-least-once + idempotent ก็รอดอยู่แล้ว

### 12.3 Monitoring (ต้องมี)
- **outbox lag** — `COUNT(*) WHERE published_at IS NULL` (ถ้าโต = relay/bus มีปัญหา)
- **consumer lag** — message ค้างใน topic เท่าไหร่
- **dead-letter count** — มี poison message ไหม
- **relay attempts** — `MAX(attempts)` สูง = publish มีปัญหา

---

<a name="13-schema"></a>
## 13. Schema Evolution

| การเปลี่ยน | breaking? | ทำยังไง |
|---|---|---|
| เพิ่ม field (optional) | ❌ ปลอดภัย | consumer เก่าไม่สนใจ field ใหม่ |
| ลบ field / เปลี่ยน type | ✅ breaking | **bump version** + consumer รองรับทั้ง 2 version ช่วง migrate |
| เปลี่ยนความหมาย field | ✅ breaking | สร้าง event type ใหม่ดีกว่า |

```go
// consumer รองรับหลาย version ช่วง migrate
switch env.Version {
case 1: handleV1(env)
case 2: handleV2(env)
}
```
> **กฎ:** เพิ่ม = ปลอดภัย · ลบ/เปลี่ยน = bump version + backward-compat · อย่า break consumer ที่ deploy ไปแล้ว

---

<a name="14-projection"></a>
## 14. Read-Projection — sync data ข้าม service

### ปัญหา
Service B อยากใช้ข้อมูลของ Service A (เช่น warehouse อยากได้ชื่อ user จาก HR) แต่ **คนละ DB · join ข้าม DB ไม่ได้**

### ทางเลือก
| แบบ | กลไก | ข้อดี/เสีย |
|---|---|---|
| **A. Read-projection** (แนะนำ) | A publish event → B เก็บ cache read-only (sync ผ่าน event) | join ใน SQL ได้ · เร็ว · decoupled · ต้องทำ consumer + backfill |
| B. Call API ตอน query | B เรียก A API ทุกครั้งที่ต้องใช้ | ไม่มี data ซ้ำ · แต่ N+1 · ช้า · B ล่มตาม A |

### Read-projection mechanics
```
HR (source of truth)              Warehouse (projection · read-only)
─────────────────────             ──────────────────────────────────
users table  ──user.updated──►    hr_users table (cache)
                  (Kafka)            ↑ เขียนจาก consumer เท่านั้น
                                     ↑ business code อ่านได้ · ห้ามเขียน
```

**กฎ projection:**
- เป็น **read-only cache** · เขียนจาก consumer เท่านั้น · มี marker ชัดว่า "ไม่ใช่ source of truth"
- **idempotent UPSERT by PK** (event ซ้ำได้)
- **initial backfill** — ตอน deploy ครั้งแรก projection ว่าง → ดึง snapshot จาก source ผ่าน internal API ครั้งเดียว แล้วใช้ event ต่อ
- หรือ **log compaction** (Kafka) / **snapshot replay** — consumer ใหม่ replay จาก offset 0 สร้าง state เอง

---

<a name="15-checklist"></a>
## 15. Checklist — เพิ่ม pattern นี้ใน project ใหม่

### Producer side
- [ ] สร้างตาราง `event_outbox` (+ partial index บน unpublished)
- [ ] เขียน `enqueueEvent(ctx, tx, ...)` — INSERT outbox รับ tx
- [ ] ทุก business write ที่ต้อง publish → เรียก `enqueueEvent` ใน tx เดียวกัน
- [ ] เขียน relay goroutine (poll → publish → mark) · start ใน main
- [ ] abstract bus ด้วย interface (Publisher) — กัน lock-in
- [ ] janitor ลบ outbox เก่า (cron)

### Event contract
- [ ] กำหนด Envelope shape (id, type, version, source, payload)
- [ ] กำหนด topic + event type naming (`<entity>.<verb-past>`)
- [ ] กำหนด payload struct ต่อ event type
- [ ] เขียน contract เป็น shared module (ใช้ร่วมทั้ง producer + consumer)

### Consumer side
- [ ] subscribe topic ด้วย consumer group ของตัวเอง
- [ ] handler **idempotent** (UPSERT by PK หรือ เช็ค envelope.ID)
- [ ] ack/commit offset **หลัง** process สำเร็จเท่านั้น
- [ ] dead-letter handling (poison message)

### Ops
- [ ] monitor: outbox lag · consumer lag · dead-letter count
- [ ] multi-replica: SKIP LOCKED หรือ leader election
- [ ] backfill plan (ถ้าเป็น projection)

---

<a name="16-anti"></a>
## 16. Anti-Patterns (อย่าทำ)

| ❌ อย่าทำ | ทำไม | ✅ ทำแทน |
|---|---|---|
| `tx.Commit()` แล้วค่อย `bus.Publish()` | dual-write · event หายได้ | outbox ใน tx เดียวกัน |
| publish bus ตรงใน request path | request ล่ม/ช้าตาม bus | relay แยก async |
| consumer ไม่ idempotent | at-least-once → ซ้ำ → data พัง | UPSERT / เช็ค id |
| ack ก่อน process | crash → message หาย | ack หลัง process สำเร็จ |
| event เป็น command (`do.X`) | coupling · producer สั่ง consumer | event = fact (`X.happened`) |
| business code import Kafka ตรงๆ | lock-in · สลับ transport ยาก | abstract ด้วย interface (โปรเจกต์นี้แก้แล้ว 2026-06-13 → `bus.Publisher`/`Consumer` + Pub/Sub impl) |
| projection เขียนจาก business code | ขัดกับ source of truth | เขียนจาก consumer เท่านั้น |
| ไม่มี dead-letter | poison message block ทั้งคิว | DLQ หลัง N attempts |
| ไม่มี version ใน envelope | schema เปลี่ยน → consumer พัง | version + backward-compat |

---

## ภาคผนวก — Reference Implementation ในโปรเจกต์นี้

| ส่วน | ไฟล์จริง |
|---|---|
| outbox table | `db/sqlc/schema.sql` → `event_outbox` |
| enqueueEvent | `internal/adapter/postgres/outbox.go` |
| atomic business+outbox | `internal/adapter/postgres/stock_repo.go` → `CreateReceipt` |
| relay (+ dead-letter + janitor) | `internal/adapter/relay/relay.go` |
| envelope + topics + payloads | `erp_kafka_module/{envelope,topics,payloads}.go` |
| **bus interface + Kafka impl** | `erp_kafka_module/bus/{publisher,consumer}.go` |
| **bus Pub/Sub impl (+ DLQ · auto-provision)** | `erp_kafka_module/bus/pubsub.go` + `pubsub_emulator_test.go` |
| **transport selector** | `EVENT_TRANSPORT` → `bus.Configure` ใน `cmd/api/main.go` · setup = `PUBSUB_SETUP.md` |
| consumer (projection · idempotent UPSERT) | `internal/adapter/consumer/hr_sync_consumer.go` |
| consumer (side-effect · inbox dedup) | `internal/adapter/consumer/procurement_consumer.go` + `postgres/procurement_repo.go` |
| inbox table | `db/sqlc/schema.sql` → `processed_events` |
| consumer restart loop (R4) | `internal/adapter/consumer/runner.go` |
| projection repo | `internal/adapter/postgres/hr_projection_repo.go` |
| sync HTTP (auth · ตรงข้าม async) | `internal/adapter/sessioncheck/validator.go` |

> เคสจริงเต็มๆ: `HR_WAREHOUSE_COMMUNICATION.md` · prod transport: `PUBSUB_SETUP.md`