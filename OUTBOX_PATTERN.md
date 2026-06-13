# Transactional Outbox — Blueprint พร้อม implement project ใหม่

บทเรียนนี้คือ **แบบสำเร็จ (blueprint)** ของ Transactional Outbox — เขียน event ลง DB พร้อม business data ใน transaction เดียว แล้ว relay ยิงเข้า Kafka แบบ at-least-once ไม่หาย โค้ดในนี้ออกแบบให้ **copy ไปต่อยอด project ใหม่ได้เลย**

> สรุป 1 บรรทัด: เลิกยิง Kafka ตรงๆ จาก business logic (dual-write พังได้) — เขียน event ลงตาราง `outbox` ใน tx เดียวกับ business → process แยก (relay) อ่าน outbox ยิงเข้า Kafka แล้ว mark sent → ได้ทั้งคู่หรือไม่ได้ทั้งคู่ (atomic)

> อ่านคู่: `INBOX_PATTERN.md` (กันซ้ำขาเข้า — คู่กับ outbox), `CDC_DEBEZIUM.md` (ทางเลือก/upgrade), `HR_KAFKA_WIRING.md` (outbox จริงใน ERP)

---

## สารบัญ

1. [ปัญหา dual-write (ทำไมยิง Kafka ตรงๆ ไม่ได้)](#1-ปัญหา-dual-write-ทำไมยิง-kafka-ตรงๆ-ไม่ได้)
2. [Outbox ทำงานยังไง — flow เต็ม](#2-outbox-ทำงานยังไง--flow-เต็ม)
3. [Schema ตาราง outbox (production-ready)](#3-schema-ตาราง-outbox-production-ready)
4. [โค้ด 1 — Event envelope + enqueue (ฝั่ง business)](#4-โค้ด-1--event-envelope--enqueue-ฝั่ง-business)
5. [โค้ด 2 — Relay (poll → publish → mark sent)](#5-โค้ด-2--relay-poll--publish--mark-sent)
6. [โค้ด 3 — Publisher (ยิงเข้า Kafka แบบไม่หาย)](#6-โค้ด-3--publisher-ยิงเข้า-kafka-แบบไม่หาย)
7. [5 ปัญหาที่ต้องแก้ให้ถูก](#7-5-ปัญหาที่ต้องแก้ให้ถูก)
8. [Cleanup / retention — outbox โตไม่หยุด](#8-cleanup--retention--outbox-โตไม่หยุด)
9. [Monitoring](#9-monitoring)
10. [Checklist — implement project ใหม่ใน 10 ขั้น](#10-checklist--implement-project-ใหม่ใน-10-ขั้น)
11. [Map ไป ERP จริง + ทั้ง 3 บทประกอบกันยังไง](#11-map-ไป-erp-จริง--ทั้ง-3-บทประกอบกันยังไง)

---

## 1. ปัญหา dual-write (ทำไมยิง Kafka ตรงๆ ไม่ได้)

```go
// ❌ แบบที่ดูเหมือนถูก แต่พังได้
func (r *ProjectRepo) Create(ctx, in) (Project, error) {
    p, err := r.db.Insert(ctx, in)   // 1. เขียน business DB
    if err != nil { return p, err }
    producer.Produce("project.created", p)  // 2. ยิง Kafka
    return p, nil
}
```

ปัญหา: ระหว่างขั้น 1 กับ 2 ไม่ atomic — เกิดได้ 4 กรณี:

```
1 ✅  2 ✅  → ปกติ
1 ✅  2 ❌  → DB มีข้อมูล แต่ Kafka ไม่มี event  ← EVENT หาย (consumer ไม่รู้ว่ามีโครงการใหม่)
1 ❌  2 —   → ไม่ทำอะไรเลย ปกติ
1 ✅  crash หลัง commit ก่อน 2 → event หายเหมือนกรณีกลาง
```

จะสลับลำดับ (ยิง Kafka ก่อน commit DB) ก็พังอีกแบบ: Kafka มี event แต่ DB rollback → **event ผี** (consumer เชื่อว่ามีโครงการ แต่จริงๆ ไม่มี)

**รากของปัญหา:** DB transaction กับ Kafka publish เป็นคนละระบบ commit พร้อมกันแบบ atomic ไม่ได้ (ไม่มี distributed transaction ที่ practical) → ต้องหาทางทำให้ "เขียน DB" กับ "ตั้งใจส่ง event" อยู่ใน tx เดียว

---

## 2. Outbox ทำงานยังไง — flow เต็ม

กุญแจ: **ตาราง outbox อยู่ใน DB เดียวกับ business** → เขียนพร้อมกันใน tx เดียว = atomic จริง ส่วนการยิง Kafka ค่อยทำทีหลังโดย process แยก

```
┌─────────────────────────────────────────────────────────┐
│  WRITE PATH (ใน request ของ user)                        │
│                                                          │
│  BEGIN TX                                                │
│    INSERT INTO projects (...)        ← business data     │
│    INSERT INTO outbox (event, ...)   ← event (tx เดียว)  │
│  COMMIT  ←── atomic: ได้ทั้งคู่ หรือไม่ได้ทั้งคู่         │
└─────────────────────────────────────────────────────────┘
                          │
                          │  (async — คนละ process/goroutine)
                          ▼
┌─────────────────────────────────────────────────────────┐
│  RELAY (poll loop ทุก N วิ)                              │
│                                                          │
│  1. SELECT * FROM outbox                                 │
│        WHERE published_at IS NULL                        │
│        ORDER BY id LIMIT 100                             │
│        FOR UPDATE SKIP LOCKED   ← กันหลาย relay ชนกัน     │
│  2. publish เข้า Kafka (acks=all)                        │
│  3. UPDATE outbox SET published_at = now() WHERE id IN.. │
│                                                          │
│  ถ้า publish fail → ไม่ mark → รอบหน้า retry (at-least-once)│
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
                  Kafka topic (hr.projects)
                          │
                          ▼
            consumer ฝั่งรับ + INBOX (กันซ้ำ — ดู INBOX_PATTERN.md)
```

**ทำไม at-least-once ไม่ exactly-once:** ถ้า relay publish สำเร็จ แต่ crash ก่อน mark `published_at` → รอบหน้ายิงซ้ำ → consumer จะเห็น event ซ้ำ → **นี่คือเหตุผลที่ outbox ต้องคู่กับ inbox** (กันซ้ำฝั่งรับ) ถึงจะได้ effectively-once ทั้ง pipeline

---

## 3. Schema ตาราง outbox (production-ready)

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id             BIGSERIAL PRIMARY KEY,        -- ordering + cursor (ลำดับการเขียน)
    aggregate_type TEXT        NOT NULL,         -- "project" → ใช้ route topic / EventRouter
    aggregate_id   TEXT        NOT NULL,         -- "42" → ใช้เป็น Kafka key (ordering ต่อ entity)
    event_type     TEXT        NOT NULL,         -- "project.created" / "project.updated"
    payload        JSONB       NOT NULL,         -- เนื้อ event (business data)
    headers        JSONB,                        -- trace id, version ฯลฯ (optional)
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ                    -- NULL = ยังไม่ส่ง, มีค่า = ส่งแล้ว
);

-- index สำคัญ: relay query เฉพาะที่ยังไม่ส่ง เรียงตาม id
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;   -- partial index = เล็ก+เร็ว
```

ทางเลือกออกแบบ:
- **`id BIGSERIAL`** — ให้ ordering ตามลำดับการ insert (สำคัญถ้าต้องรักษาลำดับ event)
- **`aggregate_id` เป็น Kafka key** — event ของ entity เดียวกันลง partition เดียวกัน → ordering ต่อ entity (Kafka รับประกัน order ภายใน partition)
- **`published_at` แทนการ DELETE ทันที** — เก็บไว้ debug/audit ก่อน แล้วค่อย cleanup เป็น batch (ดู §8) บาง design ลบทิ้งเลยหลังส่งก็ได้ถ้าไม่ต้องการ audit
- **partial index** `WHERE published_at IS NULL` — relay scan เฉพาะแถวที่ยังไม่ส่ง ไม่ต้อง scan ทั้งตารางที่อาจมีล้านแถว

---

## 4. โค้ด 1 — Event envelope + enqueue (ฝั่ง business)

แยกเป็น package `eventpub` (ตรงกับที่ ERP ใช้) ให้ business เรียกง่ายๆ

```go
package eventpub

import (
    "context"
    "database/sql"
    "encoding/json"
)

// Execer = อะไรก็ได้ที่ exec SQL ได้ — รับทั้ง *sqlx.Tx และ *sqlx.DB
// ★ จุดสำคัญ: รับ tx เข้ามา เพื่อ enqueue ใน tx เดียวกับ business
type Execer interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Enqueue เขียน 1 event ลง outbox — ต้องถูกเรียก "ภายใน tx ของ business"
func Enqueue(ctx context.Context, tx Execer, aggType, aggID, eventType string, payload any) error {
    data, err := json.Marshal(payload)
    if err != nil {
        return err
    }
    _, err = tx.ExecContext(ctx, `
        INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
        VALUES ($1, $2, $3, $4)`,
        aggType, aggID, eventType, data,
    )
    return err
}

// helper เฉพาะ domain — ให้ business เรียกอ่านง่าย
func ProjectCreated(ctx context.Context, tx Execer, p ProjectPayload) error {
    return Enqueue(ctx, tx, "project", p.ProjectID, "project.created", p)
}
func ProjectUpdated(ctx context.Context, tx Execer, p ProjectPayload) error {
    return Enqueue(ctx, tx, "project", p.ProjectID, "project.updated", p)
}
```

ฝั่ง business repo — **ห่อ business write + enqueue ใน tx เดียว** (นี่คือหัวใจ):

```go
func (r *ProjectRepo) Create(ctx context.Context, in CreateProjectInput) (Project, error) {
    tx, err := r.db.BeginTxx(ctx, nil)
    if err != nil {
        return Project{}, err
    }
    defer tx.Rollback()   // ถ้า return ก่อน Commit → rollback อัตโนมัติ (no-op หลัง commit)

    // 1. business write
    var p Project
    err = tx.GetContext(ctx, &p,
        `INSERT INTO projects (...) VALUES (...) RETURNING ...`, in.Fields()...)
    if err != nil {
        return Project{}, err
    }

    // 2. enqueue event — tx เดียวกัน
    if err := eventpub.ProjectCreated(ctx, tx, toPayload(p)); err != nil {
        return Project{}, err   // enqueue พลาด → rollback ทั้งก้อน → business ก็ไม่เกิด (atomic)
    }

    // 3. commit พร้อมกัน
    return p, tx.Commit()
}
```

> ใช้ `TxClosure` ที่มีอยู่แล้วใน `event-repo.go` ห่อก็ได้ — pattern เดียวกัน (begin → defer rollback/commit) แค่ใส่ enqueue เข้าไปใน closure

---

## 5. โค้ด 2 — Relay (poll → publish → mark sent)

process แยกที่ดึง event ออกจาก outbox ยิงเข้า Kafka — รันเป็น goroutine ใน service เดียวกัน หรือแยก deployment ก็ได้

```go
package eventpub

import (
    "context"
    "time"

    "github.com/jmoiron/sqlx"
    "github.com/sirupsen/logrus"
)

type Publisher interface {
    Publish(ctx context.Context, topic, key string, payload []byte) error
}

type Relay struct {
    db        *sqlx.DB
    publisher Publisher
    interval  time.Duration
    batchSize int
}

func NewRelay(db *sqlx.DB, pub Publisher, interval time.Duration) *Relay {
    return &Relay{db: db, publisher: pub, interval: interval, batchSize: 100}
}

func (r *Relay) Run(ctx context.Context) {
    ticker := time.NewTicker(r.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return                          // graceful shutdown
        case <-ticker.C:
            if err := r.drain(ctx); err != nil {
                logrus.Errorf("outbox relay drain error: %v", err)
            }
        }
    }
}

type outboxRow struct {
    ID            int64  `db:"id"`
    AggregateType string `db:"aggregate_type"`
    AggregateID   string `db:"aggregate_id"`
    EventType     string `db:"event_type"`
    Payload       []byte `db:"payload"`
}

// drain ดึง batch → publish → mark sent ใน tx เดียว
func (r *Relay) drain(ctx context.Context) error {
    tx, err := r.db.BeginTxx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    var rows []outboxRow
    // ★ FOR UPDATE SKIP LOCKED — ให้รัน relay หลายตัวพร้อมกันได้ ไม่ดึง row ซ้ำกัน
    err = tx.SelectContext(ctx, &rows, `
        SELECT id, aggregate_type, aggregate_id, event_type, payload
        FROM outbox
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED`, r.batchSize)
    if err != nil {
        return err
    }
    if len(rows) == 0 {
        return nil
    }

    publishedIDs := make([]int64, 0, len(rows))
    for _, row := range rows {
        topic := "hr." + row.AggregateType + "s"          // project → hr.projects
        // key = aggregate_id → ordering ต่อ entity (event เดียวกันลง partition เดียว)
        if err := r.publisher.Publish(ctx, topic, row.AggregateID, row.Payload); err != nil {
            // publish พลาด → หยุดที่ตัวนี้ ไม่ mark → รอบหน้า retry
            // หยุดทั้ง batch เพื่อรักษา ordering (ไม่ข้ามตัวที่ fail)
            break
        }
        publishedIDs = append(publishedIDs, row.ID)
    }

    if len(publishedIDs) == 0 {
        return nil   // ตัวแรกก็ fail → ไม่ commit อะไร รอบหน้าค่อยลองใหม่
    }

    // mark เฉพาะที่ publish สำเร็จ
    q, args, _ := sqlx.In(
        `UPDATE outbox SET published_at = now() WHERE id IN (?)`, publishedIDs)
    q = tx.Rebind(q)
    if _, err := tx.ExecContext(ctx, q, args...); err != nil {
        return err   // mark พลาด → rollback → รอบหน้ายิงซ้ำ (at-least-once → inbox กัน)
    }
    return tx.Commit()
}
```

จุดสำคัญในโค้ดนี้:
- **`FOR UPDATE SKIP LOCKED`** — รัน relay หลาย instance ขนานได้ (HA) แต่ละตัวล็อก row คนละชุด ไม่ดึงซ้ำ
- **break เมื่อ publish fail** — หยุดทั้ง batch ไม่ข้ามตัวที่พัง เพื่อรักษา ordering (ตัว N+1 ไม่ควรส่งก่อนตัว N ที่ fail)
- **publish ก่อน → mark ใน tx ทีหลัง** — ถ้า crash ระหว่างนี้ก็ยิงซ้ำได้ (at-least-once) ปลอดภัยกว่า mark ก่อนแล้วพัง (event หาย)

---

## 6. โค้ด 3 — Publisher (ยิงเข้า Kafka แบบไม่หาย)

Publisher ต้องตั้ง config ให้ "ไม่หาย ไม่ซ้ำตอนส่ง" — ใช้ `acks=all` + idempotent producer

```go
package eventpub

import (
    "context"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaPublisher struct {
    producer *kafka.Producer
}

func NewKafkaPublisher(brokers string) (*KafkaPublisher, error) {
    p, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": brokers,
        "acks":              "all",   // ★ รอทุก in-sync replica ยืนยัน → ไม่หายแม้ broker ตาย
        "enable.idempotence": true,   // ★ กัน producer ส่งซ้ำตอน retry (dedup ที่ broker)
        "retries":            10,
        "max.in.flight.requests.per.connection": 5,  // idempotence รองรับ ≤5 โดยยังคง ordering
    })
    if err != nil {
        return nil, err
    }
    return &KafkaPublisher{producer: p}, nil
}

func (k *KafkaPublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
    deliveryCh := make(chan kafka.Event, 1)
    err := k.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Key:            []byte(key),     // key เดียวกัน → partition เดียวกัน → ordering
        Value:          payload,
    }, deliveryCh)
    if err != nil {
        return err
    }
    // ★ รอ delivery report จริง — ยืนยันว่าถึง Kafka แล้วค่อยให้ relay mark sent
    select {
    case e := <-deliveryCh:
        m := e.(*kafka.Message)
        return m.TopicPartition.Error
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (k *KafkaPublisher) Close() {
    k.producer.Flush(5000)   // ★ graceful: รอ message ค้างส่งให้หมดก่อนปิด
    k.producer.Close()
}
```

> ⚠️ การ **รอ delivery report แบบ synchronous** (block จนกว่าจะถึง Kafka) สำคัญมากกับ outbox — ถ้าไม่รอ relay จะ mark sent ทั้งที่ message อาจยังไม่ถึง Kafka → event หาย ยอม throughput ต่ำลงเพื่อความถูกต้อง (หรือทำ async + batch ack ถ้าต้องการเร็ว แต่ซับซ้อนขึ้น)

---

## 7. 5 ปัญหาที่ต้องแก้ให้ถูก

| # | ปัญหา | วิธีแก้ในโค้ดข้างบน |
|---|---|---|
| 1 | **Ordering** — event ของ entity เดียวต้องเรียง | `ORDER BY id` + Kafka key = `aggregate_id` (partition เดียว) + break เมื่อ fail |
| 2 | **Duplicate publish** — crash หลัง publish ก่อน mark | `enable.idempotence` ฝั่ง producer (broker dedup) + inbox ฝั่ง consumer |
| 3 | **Relay ชนกัน** (หลาย instance) | `FOR UPDATE SKIP LOCKED` — ล็อก row คนละชุด |
| 4 | **Lost on crash** — mark ก่อน publish | publish (รอ ack) ก่อน → mark ใน tx ทีหลัง |
| 5 | **Outbox โตไม่หยุด** | cleanup batch (ดู §8) |

**ลำดับการรับประกัน:** business + outbox (atomic, ไม่หาย) → relay (at-least-once, อาจซ้ำ) → producer idempotence (กันซ้ำชั้น broker) → inbox consumer (กันซ้ำชั้น app) = **effectively-once ทั้ง pipeline**

---

## 8. Cleanup / retention — outbox โตไม่หยุด

`published_at IS NOT NULL` คือ row ที่ส่งแล้ว ไม่ต้องเก็บตลอด — ลบเป็น batch:

```sql
-- ลบ event ที่ส่งแล้วเกิน 7 วัน (เก็บไว้ debug ช่วงสั้นๆ)
DELETE FROM outbox
WHERE published_at IS NOT NULL
  AND published_at < now() - INTERVAL '7 days';
```

รันเป็น scheduled job (cron / pg_cron / goroutine แยก) ทุกชั่วโมง/วัน

ทางเลือก: ถ้าไม่ต้องการ audit เลย ให้ relay `DELETE` แทน `UPDATE published_at` ทันทีหลังส่งสำเร็จ — ตาราง outbox จะเล็กตลอด (มีแต่ที่ยังไม่ส่ง) แต่เสีย audit trail

---

## 9. Monitoring

ตัวชี้สุขภาพ outbox ที่ต้อง watch:

```sql
-- 1. backlog: event ที่ค้างยังไม่ส่ง (ควรใกล้ 0 — ถ้าโตเรื่อยๆ = relay ตาย/ช้า)
SELECT count(*) FROM outbox WHERE published_at IS NULL;

-- 2. lag: event เก่าสุดที่ยังไม่ส่ง ค้างมานานแค่ไหน (ควร < ไม่กี่วินาที)
SELECT now() - min(created_at) FROM outbox WHERE published_at IS NULL;
```

- **backlog โต** = relay หยุด/ช้า/Kafka down → alert
- **lag สูง** = ส่งไม่ทันที่เขียน → เพิ่ม relay instance หรือ batch size
- export 2 ค่านี้เข้า Prometheus → กราฟ + alert (เหมือน consumer lag ฝั่งรับใน Phase 3)

---

## 10. Checklist — implement project ใหม่ใน 10 ขั้น

```
□  1. migration: CREATE TABLE outbox + partial index (§3)
□  2. package eventpub: Enqueue() + helper ต่อ domain (§4)
□  3. แก้ business repo ทุกตัวที่ต้อง emit event → ห่อ tx + Enqueue (§4)
□  4. Publisher: acks=all + enable.idempotence + รอ delivery report (§6)
□  5. Relay: poll + FOR UPDATE SKIP LOCKED + break-on-fail + mark (§5)
□  6. main: เริ่ม relay เป็น goroutine + ส่ง ctx สำหรับ graceful shutdown
□  7. graceful shutdown: ctx cancel → relay หยุด → publisher.Flush()+Close()
□  8. consumer ฝั่งรับ: ใส่ INBOX กันซ้ำ (INBOX_PATTERN.md) ← อย่าลืม! outbox ลำพังยังซ้ำได้
□  9. cleanup job: ลบ published_at เก่า (§8)
□ 10. monitoring: backlog + lag → alert (§9)
```

> ขั้น 8 สำคัญสุดที่คนลืม — outbox อย่างเดียว = at-least-once (ซ้ำได้) ต้องมี inbox ฝั่งรับถึงจะปลอดภัยจริง

---

## 11. Map ไป ERP จริง + ทั้ง 3 บทประกอบกันยังไง

### 11.1 ERP ใช้อยู่แล้ว (`HR_KAFKA_WIRING.md`)

โครงตรงกับบทนี้เป๊ะ — ต่างแค่ stack:

| บทเรียนนี้ (golang_kafka) | ERP จริง | หมายเหตุ |
|---|---|---|
| `sqlx.Tx` / `sqlx.DB` | `pgx.Tx` / `*pgxpool.Pool` | `eventpub.Execer` ของ ERP รับทั้งคู่ — concept เดียวกัน |
| confluent-kafka-go Publisher | `bus.NewPublisher` (segmentio/kafka-go) | API ต่าง หลักการ acks/idempotence เหมือนกัน |
| `eventpub.Enqueue` + ProjectCreated | `eventpub.ProjectCreated(ctx, tx, payload)` | ตรงกัน ✅ |
| `Relay.Run(ctx)` poll | `eventpub.NewRelay(pool, publisher, 2s).Run(ctx)` | ตรงกัน ✅ |
| migration §3 | `0092_event_outbox.up.sql` | ตรงกัน ✅ |

สิ่งที่ ERP **ยังไม่ทำ** (จาก HR_KAFKA_WIRING) ที่บทนี้เติมให้:
- **graceful shutdown** ของ relay (ขั้น 7) — ตรวจว่า `rl.Run(ctx)` รับ ctx ที่ cancel ตอน SIGTERM จริง
- **inbox ฝั่ง inventory** (ขั้น 8) — ตอนนี้ inventory พึ่ง upsert (พอสำหรับ projection แต่ถ้ามี side-effect อื่นต้องมี inbox — ดู `INBOX_PATTERN.md §7`)
- **cleanup + monitoring** (ขั้น 9-10) — ตรวจว่ามี job ลบ + alert backlog
- **backfill snapshot ครั้งแรก** — HR_KAFKA_WIRING ระบุว่ายังไม่ทำ → ทางเลือกคือย้ายไป CDC ที่ snapshot อัตโนมัติ (`CDC_DEBEZIUM.md §5`)

### 11.2 ทั้ง 3 บท (Phase 1) ประกอบกันเป็นภาพเดียว

```
   Service A (HR)                                Service B (inventory)
┌──────────────────────┐                      ┌──────────────────────────┐
│ business + Enqueue    │  OUTBOX_PATTERN.md   │ consume → INBOX dedup     │
│ (tx เดียว, ไม่หาย)    │ ───────────────────▶ │ → business (กันซ้ำ)       │
│         │             │      Kafka           │      INBOX_PATTERN.md     │
│         ▼             │   hr.projects        └──────────────────────────┘
│  Relay poll → publish │
│  (at-least-once)      │   ← หรือเปลี่ยน relay เป็น Debezium EventRouter
└──────────────────────┘      อ่านตาราง outbox แทน → CDC_DEBEZIUM.md §7
   └──── OUTBOX กันหายขาออก + INBOX กันซ้ำขาเข้า = effectively-once ───┘
```

- **OUTBOX** (บทนี้) — กัน event หายตอนเขียน DB + ยิง Kafka (atomic)
- **INBOX** (`INBOX_PATTERN.md`) — กัน consumer process ซ้ำ (เพราะ outbox = at-least-once)
- **CDC** (`CDC_DEBEZIUM.md`) — ทางเลือก/upgrade ของ relay: ให้ Debezium อ่านตาราง outbox แทนเขียน relay เอง

---

## สรุป

- **Outbox** = เขียน event ลง DB (ตาราง outbox) ใน tx เดียวกับ business → atomic ไม่มี dual-write → relay ยิงเข้า Kafka ทีหลัง
- **โค้ด 3 ก้อน:** `Enqueue` (ฝั่ง business, รับ tx), `Relay` (poll + SKIP LOCKED + break-on-fail), `Publisher` (acks=all + idempotence + รอ ack)
- **5 ปัญหาต้องแก้:** ordering, duplicate, relay ชนกัน, lost-on-crash, ตารางโต
- **at-least-once** → ต้องคู่กับ **inbox** ถึงได้ effectively-once (ขั้น 8 ในchecklist — คนลืมบ่อย)
- **implement project ใหม่:** ทำตาม checklist 10 ขั้น §10
- **ERP:** โครงมีแล้ว เติม graceful shutdown + inbox + cleanup + monitoring ให้ครบ
- **upgrade path:** relay เริ่มเป็นภาระ → เปลี่ยนเป็น Debezium EventRouter อ่านตาราง outbox (CDC_DEBEZIUM.md §7)

> Phase 1 (Inbox + Outbox + CDC) ครบแล้ว — ถัดไป Phase 2 production hardening (security/shutdown/RF/error) ใน `LEARNING_ROADMAP.md`
