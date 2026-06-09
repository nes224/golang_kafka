# golang_kafka — Mechanics & Rebuild Guide

เอกสารนี้อธิบายว่าโปรเจกต์นี้ทำงานยังไง (mechanics) และจะ **สร้างขึ้นใหม่ตั้งแต่ศูนย์** ได้ยังไง

> สรุปสั้น: ระบบจำลอง event pipeline — producer ยิง event ทุกวินาทีเข้า Kafka → consumer อ่านมา process แบบขนาน (goroutine) → เขียนลง PostgreSQL ใน transaction (idempotent) → commit offset แบบ **sequential** (ปลอดภัยต่อ crash) ทุก 15 วินาที

---

## สารบัญ

1. [ภาพรวมระบบ](#1-ภาพรวมระบบ)
2. [Data Flow](#2-data-flow)
3. [โครงสร้างไฟล์](#3-โครงสร้างไฟล์)
4. [Mechanics ทีละ package](#4-mechanics-ทีละ-package)
5. [กลไกสำคัญเชิงลึก](#5-กลไกสำคัญเชิงลึก)
6. [Rebuild from scratch](#6-rebuild-from-scratch)
7. [ข้อควรระวัง / Known issues](#7-ข้อควรระวัง--known-issues)
8. [Scaling to production](#8-scaling-to-production)

---

## 1. ภาพรวมระบบ

โปรเจกต์เป็น Go application ตัวเดียวที่รันทั้ง producer และ consumer พร้อมกัน เพื่อสาธิตกลไกหลักของ Kafka:

- **Producer** สร้าง `Event` ใหม่ทุก 1 วินาที → marshal เป็น JSON → ยิงเข้า topic `local_topic`
- **Consumer** อ่าน message → แตก goroutine ประมวลผลแต่ละตัว (async) → เขียนลง DB
- **Commit** ไม่ใช้ auto-commit แต่เขียน loop เอง commit แบบ sequential ทุก 15 วินาที เพื่อกัน message หายตอน crash

โครงสร้างพึ่งพา (dependency):

```
Kafka (KRaft, docker)      ← message broker
PostgreSQL (docker/local)  ← เก็บ event ที่ process แล้ว
Go app                     ← producer + consumer ในตัวเดียว
```

---

## 2. Data Flow

```
┌───────────────┐  ทุก 1 วิ          ┌──────────────────┐
│  produceMsg   │  NewEvent()        │   KAFKA TOPIC    │
│  (goroutine)  │ ─marshal JSON──▶   │   "local_topic"  │
└───────────────┘   Producer()       │   [0][1][2]...   │
                                      └────────┬─────────┘
                                               │ ReadMessage()
                                               ▼
                                      ┌──────────────────┐
                                      │   readMsgLoop    │  (goroutine)
                                      │  - appendMsgState│  → จด offset = false ใน stateMap
                                      │  - NewMessage()  │  → unmarshal JSON เป็น Event
                                      └────────┬─────────┘
                                               │ msgCH <- payload
                                               ▼
                              ┌────────────────────────────────┐
                  main loop:  │  for msg := range s.msgCH       │
                              │      go s.handleMsg(msg)        │  ← fan-out ขนาน
                              └────────────────┬───────────────┘
                                               ▼
                                      ┌──────────────────┐
                                      │   saveToDB       │
                                      │  TxClosure:      │
                                      │   - Get (เช็คซ้ำ)│  ← idempotent
                                      │   - Insert       │
                                      │   - MarkAsComplete(offset) → stateMap[offset]=true
                                      └──────────────────┘

  ┌─────────────────────────────────────────────────────────┐
  │  commitOffsetLoop (goroutine แยก) — ทุก 15 วิ            │
  │   ส่อง stateMap → หา offset ต่อเนื่องสูงสุดที่ true ครบ   │
  │   → CommitOffsets(...)                                    │
  └─────────────────────────────────────────────────────────┘
```

จุดสำคัญ: การ **อ่าน** (readMsgLoop), การ **process** (handleMsg goroutine), และการ **commit** (commitOffsetLoop) ทำงานแยกกัน 3 เส้น ไม่บล็อกกัน เชื่อมกันผ่าน `msgCH` (channel) และ `msgsStateMap` (shared state ที่ป้องกันด้วย mutex)

---

## 3. โครงสร้างไฟล์

```
golang_kafka/
├── cmd/
│   └── main.go                  ← entry point: ประกอบทุกอย่าง + รัน loop หลัก
├── internal/
│   ├── shared/
│   │   ├── kafka-config.go       ← config กลาง (topic, group, host)
│   │   └── types.go              ← Message struct + NewMessage (unmarshal)
│   ├── producer/
│   │   └── producer.go           ← ห่อ kafka.Producer
│   ├── consumer/
│   │   └── consumer.go           ← ห่อ kafka.Consumer + sequential commit
│   └── repo/
│       ├── db.go                 ← เชื่อม PostgreSQL (sqlx) + util
│       └── event-repo.go         ← Event struct + CRUD + TxClosure
├── kafka.yaml                    ← docker compose (Kafka KRaft + kafdrop)
├── Makefile
└── go.mod                        ← module: github.com/golang_kafka
```

หลักการ: `internal/` แยกตามความรับผิดชอบ — producer/consumer ห่อ Kafka, repo ห่อ DB, shared เป็นของกลาง, cmd แค่ประกอบ

---

## 4. Mechanics ทีละ package

### 4.1 `shared/kafka-config.go` — config กลาง

```go
type KafkaConfig struct {
    Topic         string
    ConsumerGroup string
    Host          string
}
// Topic: "local_topic", ConsumerGroup: "local_cg", Host: "localhost"
```

แหล่งความจริงเดียว (single source of truth) — producer/consumer ดึงค่าจากที่นี่ทั้งคู่ แก้ที่เดียวมีผลทั้งระบบ

### 4.2 `shared/types.go` — สัญญารูปแบบ message

```go
type Message struct {
    Metadata *kafka.TopicPartition  // topic/partition/offset ของ message นี้
    Event    *repo.Event            // เนื้อหาที่ unmarshal แล้ว
}

func NewMessage(metadata *kafka.TopicPartition, data []byte) *Message {
    e := &repo.Event{}
    json.Unmarshal(data, e)   // byte → Event
    return &Message{Metadata: metadata, Event: e}
}
```

`Message` มัด 2 อย่างเข้าด้วยกัน: **metadata** (ใช้ตอน commit — ต้องรู้ offset) + **event** (เนื้อหาจริงที่ไป process). นี่คือเหตุผลที่ producer ต้องส่ง JSON ของ `Event` ไม่ใช่ plain text — ไม่งั้น `json.Unmarshal` ตรงนี้พัง

### 4.3 `producer/producer.go` — ฝั่งยิง

```go
func NewKafkaProducer(topic string) *KafkaProducer {
    p, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
    // ❌ ห้าม defer p.Close() ตรงนี้ — constructor return ปุ๊บ producer ปิดทันที

    go func() {                          // delivery report handler
        for e := range p.Events() {
            // log ว่า message ส่งถึง Kafka สำเร็จ/ล้มเหลว
        }
    }()
    return &KafkaProducer{producer: p, topic: topic}
}

func (p *KafkaProducer) Producer(msg string) {
    p.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
        Value:          []byte(msg),
    }, nil)
}
```

- **delivery report goroutine** — `Produce()` เป็น async (โยน message เข้า queue แล้ว return เลย) ผลลัพธ์การส่งจริงมาทาง `p.Events()` ทีหลัง goroutine นี้คอยฟัง
- **`PartitionAny`** — ให้ Kafka เลือก partition เอง (ไม่ได้ระบุ key)
- **`Close()`** ทำ `Flush(5000)` รอ message ค้างส่งให้หมดก่อนปิด — ไว้เรียกตอน shutdown

### 4.4 `consumer/consumer.go` — ฝั่งรับ + หัวใจของ sequential commit

โครงสร้าง state สำคัญ:

```go
type KafkaConsumer struct {
    Consumer     *kafka.Consumer
    msgCH        chan *shared.Message
    msgsStateMap map[kafka.Offset]bool   // offset → process เสร็จยัง (true/false)
    lastCommited kafka.Offset            // commit ไปถึง offset ไหนแล้ว
    maxReceived  *kafka.TopicPartition   // offset สูงสุดที่อ่านมา
    mu           *sync.RWMutex           // ป้องกัน stateMap จากหลาย goroutine
    commitDur    time.Duration           // 15 วินาที
}
```

**config สำคัญ** — ปิด auto-commit:

```go
kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":  cfg.Host,
    "group.id":           cfg.ConsumerGroup,
    "enable.auto.commit": false,   // ← คุม commit เองทั้งหมด
})
```

**ตอน start** — อ่าน committed offset เดิมมาเป็นจุดเริ่ม:

```go
commited, _ := c.Committed([]kafka.TopicPartition{tp}, 5000)
latestComm := commited[len(commited)-1].Offset   // เริ่มต่อจากที่เคย commit
```

มี 3 goroutine ทำงานพร้อมกัน:

```go
go consumer.commitOffsetLoop()   // commit ทุก 15 วิ
go consumer.checkReadyToAccept() // เช็คว่าได้ partition แล้วยัง
go consumer.readMsgLoop()        // อ่าน message
```

**`readMsgLoop`** — อ่านแล้วจด state + ส่งเข้า channel:

```go
for {
    msg, err := c.Consumer.ReadMessage(time.Second)
    if err != nil {
        if kerr, ok := err.(kafka.Error); ok && kerr.IsTimeout() {
            continue   // timeout = ปกติ ไม่มี message ช่วงนั้น
        }
        fmt.Printf("Consumer error: %v\n", err)
        continue
    }
    c.appendMsgState(&msg.TopicPartition)        // จด offset = false (ยังไม่เสร็จ)
    payload := shared.NewMessage(&msg.TopicPartition, msg.Value)
    c.msgCH <- payload                            // โยนเข้า channel ให้ handler
}
```

**`appendMsgState` / `MarkAsComplete`** — จด/อัปเดต state (มี lock):

```go
func (c *KafkaConsumer) appendMsgState(tp *kafka.TopicPartition) {
    c.mu.Lock(); defer c.mu.Unlock()
    c.msgsStateMap[tp.Offset] = false            // เพิ่งอ่านมา ยังไม่ process
    if c.maxReceived.Offset < tp.Offset {        // อัปเดต offset สูงสุด
        c.maxReceived = ...tp...
    }
}

func (c *KafkaConsumer) MarkAsComplete(tp *kafka.TopicPartition) {
    c.mu.Lock(); defer c.mu.Unlock()
    c.msgsStateMap[tp.Offset] = true             // process เสร็จแล้ว
}
```

**`commitOffsetLoop`** — หัวใจ sequential commit (ทุก 15 วิ):

```go
for offset := c.lastCommited; offset < c.maxReceived.Offset; offset++ {
    completed, exists := c.msgsStateMap[offset]
    if !exists { continue }
    if completed {
        delete(c.msgsStateMap, offset)   // เสร็จแล้ว เก็บกวาด
        continue
    }
    // เจอตัวแรกที่ยัง false → หยุดตรงนี้ commit ได้แค่ถึงก่อนหน้า
    latestToCommit.Offset = offset
    break
}
c.Consumer.CommitOffsets([]kafka.TopicPartition{latestToCommit})
```

ตรรกะคือ ไล่จาก `lastCommited` ขึ้นไป เจอ offset แรกที่ยังไม่เสร็จ (`false`) ก็ commit ได้แค่ถึงตรงนั้น — **กระโดดข้ามรูโหว่ไม่ได้** (ดู §5.1)

### 4.5 `repo/db.go` — เชื่อม PostgreSQL

```go
import _ "github.com/lib/pq"   // blank import: register driver "postgres"

func NewDBConn() (*sqlx.DB, error) {
    return sqlx.Connect("postgres", getDBConnString())
}
// conn string: host=localhost port=5433 user=alphamech password=... dbname=kafka_yt sslmode=disable
```

- **blank import `_`** — ต้องมีไม่งั้น `sqlx.Connect("postgres", ...)` จะ panic `unknown driver`. driver register ตัวเองผ่าน `init()` ตอนถูก import
- `GenerateRandomString` — สร้าง event id แบบสุ่ม

### 4.6 `repo/event-repo.go` — CRUD + transaction helper

```go
type Event struct {
    EventId   string    `db:"event_id"`
    EventName string    `db:"event_type"`
    Timespamp time.Time `db:"timestamp"`
}
```

**`TxClosure`** — generic helper ครอบ transaction ด้วย defer + recover:

```go
func TxClosure[T any](ctx, r, fn func(ctx, tx) (T, error)) (T, error) {
    tx, _ := r.repo.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
    defer func() {
        if r := recover(); r != nil { tx.Rollback(); panic(r) }  // panic → rollback
        if err != nil { tx.Rollback(); return }                  // error → rollback
        err = tx.Commit()                                         // สำเร็จ → commit
    }()
    res, err = fn(ctx, tx)
    return res, err
}
```

รูปแบบนี้ทำให้โค้ดที่เรียกใช้ไม่ต้องจัดการ begin/commit/rollback เอง — ส่ง closure เข้าไป ถ้า return error หรือ panic ก็ rollback อัตโนมัติ ถ้าสำเร็จก็ commit ให้

**`Get` ก่อน `Insert`** = idempotent (ดู §5.2)

### 4.7 `cmd/main.go` — ประกอบร่าง

```go
func main() {
    db, _ := repo.NewDBConn()
    er := repo.NewEventRepo(db)
    s := NewServer(er)              // สร้าง producer + consumer + msgCH

    go s.produceMsg()               // เริ่มยิง event
    for msg := range s.msgCH {      // รับ message จาก consumer
        go s.handleMsg(msg)         // แตก goroutine process แต่ละตัว (async)
    }
}
```

`handleMsg` → `saveToDB` ครอบ `TxClosure` แล้ว `defer MarkAsComplete` (จด state ว่าเสร็จ ไม่ว่า insert ผ่านหรือ skip)

---

## 5. กลไกสำคัญเชิงลึก

### 5.1 Sequential commit — ทำไม commit ข้ามรูโหว่ไม่ได้

เพราะ process ทำแบบขนาน (goroutine) message เสร็จ **ไม่เรียงลำดับ** เช่น:

```
offset:  0    1    2    3
state:   ✓    ✓    ✗    ✓     (offset 2 ยังไม่เสร็จ)
              ↑ commit ได้แค่ถึงนี่ (1) — offset 3 ที่เสร็จแล้วก็ต้องรอ
```

committed offset เป็น **เลขเดียว** ที่แปลว่า "ทุกตัวก่อนหน้าเสร็จหมดแล้ว" ถ้า commit 3 ทั้งที่ 2 ยังไม่เสร็จ แล้ว crash → restart มาอ่านที่ 4 → **offset 2 หายตลอดกาล**

เลยต้อง commit แค่ offset ที่เสร็จ **ต่อเนื่องกันโดยไม่มีรู** พอ 2 เสร็จค่อยกระโดดยาวถึง 3

### 5.2 Idempotent consumer — กันทำซ้ำ

เพราะใช้ **at-least-once** (process ก่อน → commit ทีหลัง) message อาจถูกอ่านซ้ำได้ตอน crash ก่อน commit เลยต้องเช็คก่อนทำ:

```go
event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)
if event != nil {
    return "", errors.New("already existing -> skipping")  // เคยทำแล้ว ข้าม
}
id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
```

`event_id` เป็น primary key → ถ้ามีอยู่แล้วก็ไม่ insert ซ้ำ ทำให้ process ตัวเดิมกี่ครั้งผลก็เหมือนเดิม

### 5.3 Manual commit แทน auto-commit

`enable.auto.commit: false` — ปิดการ commit อัตโนมัติทั้งหมด เพราะ auto-commit (default ทุก 5 วิ) อาจ commit ก่อน process เสร็จ → ถ้า crash จะ message หาย (at-most-once) โค้ดนี้เลยคุม commit เองผ่าน `commitOffsetLoop` ให้ commit หลังยืนยันว่า process เสร็จจริง

### 5.4 ทำไมต้อง mutex

`msgsStateMap` ถูกแตะจากหลาย goroutine พร้อมกัน — `readMsgLoop` (เขียน false), `handleMsg` หลายตัว (`MarkAsComplete` เขียน true), `commitOffsetLoop` (อ่าน + ลบ) map ใน Go ไม่ thread-safe เขียนพร้อมกันจะ panic เลยต้องล็อกด้วย `sync.RWMutex`

---

## 6. Rebuild from scratch

ทำตามลำดับนี้สร้างใหม่ได้ทั้งระบบ

### Step 0 — Prerequisites

- Go 1.18+ (ใช้ generics ใน `TxClosure`)
- Docker + Docker Compose
- `librdkafka` — confluent-kafka-go ต้องการ C library นี้ (macOS: `brew install librdkafka`)

### Step 1 — init module

```bash
mkdir golang_kafka && cd golang_kafka
go mod init github.com/golang_kafka
```

### Step 2 — ติดตั้ง dependencies

```bash
go get github.com/confluentinc/confluent-kafka-go/v2/kafka
go get github.com/jmoiron/sqlx
go get github.com/lib/pq
go get github.com/sirupsen/logrus
```

### Step 3 — ตั้ง Kafka (KRaft) ด้วย Docker

สร้าง `kafka.yaml` (KRaft mode — ไม่ต้องมี ZooKeeper):

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: kafka
    hostname: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_LISTENERS: "CONTROLLER://:29093,PLAINTEXT_INTERNAL://:29092,PLAINTEXT://:9092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT_INTERNAL"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 4
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
  kafdropui:
    image: obsidiandynamics/kafdrop
    ports: ["9090:9000"]
    environment:
      KAFKA_BROKERCONNECT: "kafka:29092"
    depends_on: [kafka]
```

```bash
docker-compose -f kafka.yaml up -d
# เปิด kafdrop ดู topic ได้ที่ http://localhost:9090
```

### Step 4 — ตั้ง PostgreSQL + สร้างตาราง

ต่อ DB (port 5433, dbname `kafka_yt`) แล้วสร้างตาราง:

```sql
CREATE TABLE IF NOT EXISTS events (
    event_id   TEXT PRIMARY KEY,
    event_type TEXT,
    timestamp  TIMESTAMPTZ
);
```

### Step 5 — เขียนโค้ดตามลำดับ

1. `internal/shared/kafka-config.go` — config
2. `internal/repo/db.go` + `event-repo.go` — DB layer (อย่าลืม blank import `_ "github.com/lib/pq"`)
3. `internal/shared/types.go` — `Message` + `NewMessage`
4. `internal/producer/producer.go` — producer (อย่า `defer p.Close()` ใน constructor)
5. `internal/consumer/consumer.go` — consumer + commit loop
6. `cmd/main.go` — ประกอบ

### Step 6 — รัน

```bash
go run ./cmd
# หรือผ่าน Makefile:
make app
```

ควรเห็น log: topic created → `Delivered message` → `MarkAsComplete` → `Commited on CRON` ทุก 15 วิ

---

## 7. ข้อควรระวัง / Known issues

จุดที่สังเกตเห็นจากโค้ดปัจจุบัน — ควรเก็บก่อนเอาไป production:

**1. `commitOffsetLoop` เสี่ยง deadlock** — ใน `select { case <-ticker.C: }` มีการ `c.mu.Lock()` แล้วบาง path `continue` โดยยังไม่ `Unlock()`:
- บรรทัด `if c.maxReceived == nil { continue }` — ถือ lock อยู่แล้ว continue
- บรรทัด `if c.lastCommited == c.maxReceived.Offset { ...; continue }` — เหมือนกัน

รอบ ticker ถัดไปจะ `Lock()` อีกครั้งบน lock ที่ยังไม่ปลด → ค้าง path ที่สองเข้าได้จริงเมื่อไม่มี message ใหม่เข้ามาช่วงนั้น ควรย้าย `Unlock` ให้ครอบทุก exit path (หรือใช้ closure + defer)

**2. `lastCommited = latestToCommit.Offset - 1`** — ตรรกะ offset ลบ 1 ตรงนี้ดูแปลก ควร trace ให้แน่ใจว่า committed position ตรงกับที่ตั้งใจ (Kafka commit คือ offset ของ "ตัวถัดไปที่จะอ่าน" ไม่ใช่ตัวล่าสุดที่อ่าน)

**3. มี partition เดียว** — `NumPartitions: 1` ตอนสร้าง topic ทำให้ขนานได้จริงแค่ระดับ goroutine ไม่ใช่ระดับ partition และ commit logic เขียนเผื่อ partition 0 ตัวเดียว ยังไม่รองรับหลาย partition

**4. `"^aRegex.*[Tt]opic"`** — subscribe regex topic ที่ไม่มีจริง ทำให้ขึ้น log `Subscribed topic not available` รก log เฉยๆ ลบได้ถ้าไม่ใช้

**5. ยังไม่มี graceful shutdown** — `exitCH` ถูกสร้างแต่ไม่มีใคร close, producer `Close()` ไม่ถูกเรียก กด Ctrl+C อาจมี message ค้างหรือ commit ไม่ทัน ควรเพิ่ม signal handler

**6. `field topic: cfg.ConsumerGroup`** — ใน consumer struct เซ็ต `topic` เป็นค่า ConsumerGroup (ดูเหมือนพิมพ์สลับ) แต่ field นี้ไม่ถูกใช้ที่ไหน เลยไม่กระทบการทำงาน

**7. `Event.Timespamp`** — สะกดผิด (ควรเป็น Timestamp) แต่ db tag ถูก (`timestamp`) เลยทำงานได้ปกติ แก้ชื่อ field ทีหลังได้

---

## 8. Scaling to production

โค้ดปัจจุบันเป็นเวอร์ชัน **1 partition / 1 consumer** ส่วนนี้อธิบายว่าระบบจะหน้าตาเป็นยังไงเมื่อ scale ขึ้นจริง (หลาย partition + หลาย pod + หลาย consumer group) และต้องปรับโค้ดอะไรบ้าง

### 8.1 กฎ partition + consumer group (5 ข้อ)

```
1. แต่ละ partition เป็นเจ้าของ offset ของตัวเอง และเรียงลำดับภายใน
2. 1 partition = 1 consumer ต่อ group  (ห้ามแชร์ partition ในกลุ่มเดียว)
3. 1 consumer ถือได้หลาย partition
4. ถ้า consumer > partition → ตัวที่เกินจะ IDLE (รอ rebalance)
5. คนละ consumer group อ่าน partition เดียวกันได้อิสระ (ต่างคนต่างจำ offset)
```

> offset นับแยกต่อ partition — P0 มี offset 0,1,2… และ P1 ก็มี 0,1,2… เป็นคนละชุดกัน ระบุ message 1 ชิ้นต้องใช้ `topic + partition + offset`

### 8.2 ภาพ scale: topic 3 partition + group `local_cg` (4 pod)

3 partition แจกให้ 3 pod, pod ที่ 4 ว่างงาน (กฎข้อ 4) แต่ละ pod ถือ **PartitionState** ของตัวเอง (= `lastCommitted` + `maxReceived` + `msgsStateMap` ใน struct `KafkaConsumer`) และรัน `commitOffsetLoop` แยกกัน:

| Pod | partition | msgsStateMap | scan เจออะไร | commit ถึง |
|---|---|---|---|---|
| Pod 3 | P2 | 1✓ 2✓ 3✓ | ต่อเนื่องครบ ไม่มีรู | offset **4** (= 3+1) |
| Pod 1 | P0 | 3⏳ 4✓ 5✓ | เจอ 3 Pending → STOP | offset **3** (4,5 ที่เสร็จต้องรอ) |
| Pod 2 | P1 | 2⏳ 3⏳ | เจอ 2 Pending → STOP | offset **2** (ไม่ขยับ) |
| Pod 4 | — | — | IDLE รอ rebalance | — |

**Pod 1 = ตัวอย่าง sequential commit เป๊ะ** — offset 4,5 process เสร็จแล้ว แต่ commit ไม่ได้เพราะ offset 3 ยัง Pending ต้องรอ 3 เสร็จก่อนถึงกระโดดยาว (ห้ามข้ามรูโหว่)

> ยืนยันอีกครั้ง: committed offset = "offset ตัวถัดไปที่จะอ่าน" ไม่ใช่ตัวล่าสุดที่ process (Pod 3 process ถึง offset 3 → commit `4`) — ตรงกับ §7 ข้อ 2

### 8.3 หลาย consumer group อ่าน topic เดียวกัน (กฎข้อ 5)

```
Topic local_topic
 ├─▶ Group local_cg     (งานหลัก — 4 pod)
 ├─▶ Group analytics_cg (เก็บสถิติ — 2 consumer, ตัวนึงถือ 2 partition)
 └─▶ Group backup_cg    (สำรอง — 1 consumer ถือทั้ง 3 partition)
```

ทั้ง 3 group อ่าน message ครบทุกตัวเหมือนกัน แต่จำ offset แยกกัน — order 1 ใบถูกประมวลผลโดยทั้ง 3 บทบาทพร้อมกัน (นี่คือหัวใจ event-driven) `backup_cg` ที่ `lastCommitted == maxReceived` ทุก partition = ตามทันงานหมดแล้ว ไม่มีค้าง

### 8.4 สิ่งที่ต้องปรับในโค้ดเพื่อรองรับหลาย partition

โค้ดปัจจุบันยังรองรับ partition เดียว จุดที่ต้องแก้:

1. **`tp := kafka.TopicPartition{..., Partition: 0}`** — hardcode partition 0 อยู่ ต้องเปลี่ยนให้ทำงานกับทุก partition ที่ถูก assign (ดูจาก `Assignment()`)
2. **`msgsStateMap map[kafka.Offset]bool`** — เป็น map รวมตัวเดียว ปัญหาคือ offset 3 ของ P0 จะชนกับ offset 3 ของ P1 ต้องเปลี่ยนเป็น **per-partition state** เช่น `map[int32]map[kafka.Offset]bool` (partition → offset → done) หรือสร้าง struct `PartitionState` แยกต่อ partition
3. **`lastCommited` / `maxReceived`** — ต้องแยกต่อ partition เช่นกัน (แต่ละ partition มีตำแหน่ง commit ของตัวเอง)
4. **`commitOffsetLoop`** — ต้อง scan + commit แยกต่อ partition (วนทุก partition ที่ถือ แล้ว commit รายตัว)
5. **rebalance handler** — เมื่อ partition ถูกแจกใหม่ (pod เพิ่ม/หาย) ต้องจัดการ state ของ partition ที่ได้รับ/เสียไป

นี่คือทิศทางการโตของโปรเจกต์ — โครงสร้าง `PartitionState` ต่อ partition คือหัวใจที่ทำให้ scale ได้ตามภาพ §8.2
