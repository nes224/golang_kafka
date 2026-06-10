# golang_kafka — Mechanics & Rebuild Guide

เอกสารนี้อธิบายว่าโปรเจกต์นี้ทำงานยังไง (mechanics) และจะ **สร้างขึ้นใหม่ตั้งแต่ศูนย์** ได้ยังไง

> สรุปสั้น: producer ยิง event ทุกวินาทีเข้า Kafka (4 partition) → consumer ที่ **รองรับ rebalance** อ่านมา process แบบขนาน (goroutine) → เขียนลง PostgreSQL ใน transaction (idempotent) → commit offset แบบ **sequential ต่อ partition** ทุก 10 วินาที → รันหลาย instance พร้อมกันได้ (scale)

> เอกสารชุดเดียวกัน: `CODE_WALKTHROUGH.md` (โค้ดทีละบรรทัด), `MESSAGE_LIFECYCLE.md` (flow), `REBALANCE.md` (rebalance เชิงลึก), `LEARNING_ROADMAP.md` (ก้าวต่อไป), `KAFKA_GLOSSARY.md` (คำศัพท์)

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

Go application ตัวเดียวที่รันทั้ง producer และ consumer พร้อมกัน สาธิตกลไก Kafka ระดับ production:

- **Producer** สร้าง `Event` ใหม่ทุก 1 วินาที → marshal JSON → ยิงเข้า topic (4 partition)
- **Consumer** รองรับ **rebalance** — อ่าน message → เก็บ state **แยกต่อ partition** → process แบบขนาน
- **Commit** ปิด auto-commit, ใช้ **sequential commit ต่อ partition** (commit loop แยกแต่ละ partition) ทุก 10 วินาที
- **Rebalance** — เปิดหลาย instance พร้อมกันได้ Kafka แจก partition ให้แต่ละตัว และ commit ก่อนปล่อย partition ตอนถูกยึด

dependency:

```
Kafka (KRaft, docker)      ← message broker
PostgreSQL (docker/local)  ← เก็บ event ที่ process แล้ว
Go app                     ← producer + consumer (รันหลาย instance ได้)
```

---

## 2. Data Flow

```
┌───────────────┐  ทุก 1 วิ           ┌──────────────────────┐
│  produceMsg   │  NewEvent()         │   KAFKA TOPIC        │
│  (goroutine)  │ ─marshal JSON──▶    │ local_topic_sticky1  │
└───────────────┘  Produce([]byte)    │  4 partitions        │
                                       └──────────┬───────────┘
                                                  │ ReadMessage()
                                                  ▼
                                       ┌──────────────────────┐
                                       │  consumeLoop          │ (RunConsumer)
                                       │  - appendMsgState     │ → state[offset]=Pending (per-partition)
                                       │  - NewMessage()       │ → unmarshal JSON เป็น Event
                                       └──────────┬────────────┘
                                                  │ MsgCH <- msg (select + timeout 5s)
                                                  ▼
                              ┌────────────────────────────────────┐
                  main loop:  │  for msg := range s.msgCH           │
                              │      go s.handleMsg(msg)            │ ← fan-out ขนาน
                              └────────────────┬───────────────────┘
                                               ▼
                                       ┌──────────────────────┐
                                       │   saveToDB (TxClosure)│
                                       │   - Get (เช็คซ้ำ)     │ ← idempotent
                                       │   - Insert            │
                                       └──────────┬────────────┘
                                                  ▼
                                       UpdateState(offset, Success/Error)  → state[offset] per-partition

  ┌─────────────────────────────────────────────────────────────────┐
  │  PartitionState.commitOffsetLoop — 1 goroutine ต่อ partition      │
  │   ทุก 10 วิ: findLatestToCommit (scan lastCommitted→maxReceived)  │
  │   เจอ Pending = หยุด → CommitOffsets ของ partition นั้น           │
  └─────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────┐
  │  rebalanceCB — เมื่อ Kafka แจก partition ใหม่                     │
  │   Assigned → สร้าง PartitionState + เปิด commit loop             │
  │   Revoked  → commit ค้างก่อน → หยุด loop → ปล่อย partition       │
  └─────────────────────────────────────────────────────────────────┘
```

การ **อ่าน** (consumeLoop), การ **process** (handleMsg goroutine), การ **commit** (commit loop ต่อ partition), และ **rebalance** (callback) ทำงานแยกกัน เชื่อมผ่าน `MsgCH` (channel) และ `msgsStateMap` (per-partition state + mutex/atomic)

---

## 3. โครงสร้างไฟล์

```
golang_kafka/
├── cmd/
│   └── main.go                   ← entry point: ประกอบ + รัน (RunConsumer + produce + handle)
├── internal/
│   ├── shared/
│   │   ├── kafka-config.go        ← config (topic, group, host, strategy, num partitions)
│   │   └── types.go               ← Message struct + NewMessage (unmarshal)
│   ├── producer/
│   │   └── producer.go            ← ห่อ kafka.Producer
│   ├── consumer/
│   │   ├── consumer.go            ← ห่อ kafka.Consumer + rebalance + read loop
│   │   └── parition-state.go      ← PartitionState + commit loop (ต่อ partition)  ★ใหม่
│   └── repo/
│       ├── db.go                  ← เชื่อม PostgreSQL (sqlx) + util
│       ├── event-repo.go          ← Event + CRUD + TxClosure
│       └── repo-err.go            ← ตรวจ duplicate key error  ★ใหม่
├── kafka.yaml                     ← docker compose (Kafka KRaft + kafdrop)
├── Makefile
└── go.mod                         ← module: github.com/golang_kafka
```

---

## 4. Mechanics ทีละ package

### 4.1 `shared/kafka-config.go` — config กลาง

```go
type KafkaPartitionStrategies string
const (
    CooperativeStickyStrategy KafkaPartitionStrategies = "cooperative-sticky"
    RoundRobin                KafkaPartitionStrategies = "roundrobin"
)

type KafkaConfig struct {
    DefaultTopic             string                    // "local_topic_sticky1"
    Host                     string                    // "localhost"
    ConsumerGroup            string                    // "local_cg1"
    ParititionAssignStrategy KafkaPartitionStrategies  // cooperative-sticky
    NumPartitions            int                       // 4
}
```

เพิ่ม **strategy** (วิธีแจก partition ตอน rebalance) และ **NumPartitions: 4** (เปิดให้ขนานได้จริงระดับ partition ไม่ใช่แค่ goroutine)

### 4.2 `shared/types.go` — สัญญารูปแบบ message

```go
type Message struct {
    Metadata *kafka.TopicPartition  // topic/partition/offset (ใช้ตอน commit + UpdateState)
    Event    *repo.Event            // เนื้อหาที่ unmarshal แล้ว
}
func NewMessage(metadata, data []byte) *Message { /* json.Unmarshal → Event */ }
```

ไม่เปลี่ยนจากเดิม — `Metadata` สำคัญขึ้นเพราะตอนนี้ใช้ระบุ partition+offset ใน `UpdateState`

### 4.3 `producer/producer.go` — ฝั่งยิง

```go
func NewKafkaProducer() *KafkaProducer { … }     // ไม่รับ arg แล้ว (ดึง topic จาก config)
func (p *KafkaProducer) Produce(msg []byte) { … } // ชื่อ Produce, รับ []byte
```

- delivery report goroutine ยังอยู่ (log ผ่าน logrus พร้อม partition+offset)
- `Produce` ใช้ `PartitionAny` → Kafka กระจาย message ไปทั้ง 4 partition เอง

### 4.4 `consumer/consumer.go` — ฝั่งรับ + rebalance

state แยกต่อ partition + enum:

```go
type MsgState = int32
const ( MsgState_Pending = iota; MsgState_Success; MsgState_Error )

type KafkaConsumer struct {
    consumer     *kafka.Consumer
    ID           string                       // id สุ่ม (ดูตอนรันหลาย instance)
    MsgCH        chan *shared.Message
    mu           *sync.RWMutex
    msgsStateMap map[int32]*PartitionState     // partition → state (แยกต่อ partition!)
    commitDur    time.Duration                 // 10s
    cfg          *shared.KafkaConfig
}
```

config สำคัญ:
```go
"enable.auto.commit":              false,                // คุม commit เอง
"auto.offset.reset":               "earliest",
"go.application.rebalance.enable": true,                 // จัดการ rebalance เอง
"partition.assignment.strategy":   "cooperative-sticky", // แจก partition แบบ incremental
```

method หลัก:
- `RunConsumer()` — เปิด `checkReadyToAccept` + `consumeLoop` (ต้องเรียกจาก main)
- `consumeLoop()` — อ่าน message → `appendMsgState` → `NewMessage` → ส่งเข้า `MsgCH` (select + timeout 5s กัน block)
- `appendMsgState()` — `state[offset]=Pending` ใน PartitionState ของ partition นั้น + อัปเดต maxReceived (atomic)
- `UpdateState(tp, newState)` — ตั้ง state เป็น Success/Error (เรียกจาก handler)
- `rebalanceCB` → `assignPrntCB` / `revokePrtnCB` — จัดการตอน partition ถูกแจก/ยึด (ดู §5.5 + REBALANCE.md)

### 4.5 `consumer/parition-state.go` — state + commit loop ต่อ partition ★

```go
type PartitionState struct {
    ID           int32
    state        map[kafka.Offset]MsgState   // offset → สถานะ ของ partition นี้
    maxReceived  *atomic.Int32
    lastCommited *atomic.Int32
    ctx, cancel  // ใช้สั่งหยุด loop ตอน revoke
    exitCH       chan struct{}
}
```

- **1 PartitionState ต่อ 1 partition** — offset แต่ละ partition นับแยก เลยต้องแยก map
- `commitOffsetLoop()` — ticker ทุก 10s → `findLatestToCommit` → `CommitOffsets`
- `findLatestToCommit()` — scan `lastCommited`→`maxReceived` เจอ Pending = หยุด (sequential), Success/Error = ลบทิ้ง+ผ่าน
- `ctx/cancel/exitCH` — ตอน revoke จะ `cancel()` แล้วรอ `<-exitCH` ให้ loop หยุดสนิทก่อน

### 4.6 `repo/db.go` + `event-repo.go` — DB layer

```go
import _ "github.com/lib/pq"                  // register driver "postgres"
func NewDBConn() (*sqlx.DB, error) { sqlx.Connect("postgres", dsn) }

type Event struct { EventId; EventName; Timespamp }   // db tags
func TxClosure[T any](ctx, r, fn) (T, error) { /* begin → defer commit/rollback/recover */ }
```

`TxClosure` = generic helper ครอบ transaction (recover → rollback, error → rollback, สำเร็จ → commit) — ไม่เปลี่ยนจากเดิม

### 4.7 `repo/repo-err.go` — ตรวจ duplicate key ★

```go
func IsDuplicateKeyErr(err error) bool {
    var pgErr *pq.Error
    if errors.As(err, &pgErr) { return pgErr.Code == "23505" }  // PG: unique_violation
    return false
}
```

ตรวจว่า insert ชน unique constraint ไหม — ใช้ทำ idempotent แบบพึ่ง DB (insert ตรงๆ ถ้าซ้ำจับ error นี้) แทนการ `Get` ก่อน

### 4.8 `cmd/main.go` — ประกอบร่าง

```go
func main() {
    db, _ := repo.NewDBConn()
    s := NewServer(repo.NewEventRepo(db))

    go s.consumer.RunConsumer()    // ★ เริ่ม consumer loop (เดิม constructor ทำให้)
    go s.produceMsg()              // ยิง event ทุกวิ
    for msg := range s.msgCH {     // รับ → process ขนาน
        go s.handleMsg(msg)
    }
}
```

`saveToDB` → `TxClosure` (Get→Insert) แล้ว `UpdateState(Success/Error)` ตามผล (duplicate = idempotent skip → Success)

---

## 5. กลไกสำคัญเชิงลึก

### 5.1 Sequential commit (ต่อ partition)

process ขนาน → เสร็จไม่เรียงลำดับ committed offset เป็นเลขเดียวที่แปลว่า "ทุกตัวก่อนหน้าเสร็จหมด" จึง commit ได้แค่ offset ที่เสร็จ **ต่อเนื่องไม่มีรู** — เจอ Pending ตัวแรกก็หยุด ตอนนี้ทำ **แยกต่อ partition** (แต่ละ partition มี commit loop + lastCommitted ของตัวเอง)

### 5.2 Idempotent consumer

at-least-once → message อาจถูกอ่านซ้ำ → `Get` ก่อน `Insert` (หรือ `IsDuplicateKeyErr`) กันทำซ้ำ — duplicate ถือว่าสำเร็จ (เคยทำแล้ว)

### 5.3 Manual commit

`enable.auto.commit: false` คุม commit เองหลัง process เสร็จจริง (กัน message หายแบบ at-most-once)

### 5.4 Mutex + Atomic

`msgsStateMap` ถูกแตะหลาย goroutine → `sync.RWMutex` ป้องกัน ส่วน `maxReceived`/`lastCommited` ใช้ `atomic.Int32` (เร็วกว่า lock สำหรับ counter เดี่ยว)

### 5.5 Rebalance — commit ก่อน revoke

เปิดหลาย instance → Kafka แจก partition ให้แต่ละตัว ตอน partition ถูกยึดคืน (revoke) ต้อง **commit offset ที่ค้างก่อนปล่อย** ไม่งั้น instance ใหม่อ่านซ้ำเยอะ ใช้ **cooperative-sticky** (แจกแบบ incremental — ปล่อยเฉพาะ partition ที่ต้องย้าย ตัวอื่นทำงานต่อ ลด downtime) — รายละเอียดเต็มใน `REBALANCE.md`

---

## 6. Rebuild from scratch

### Step 0 — Prerequisites
Go 1.18+ (generics), Docker, `librdkafka` (macOS: `brew install librdkafka`)

### Step 1 — init module
```bash
go mod init github.com/golang_kafka
```

### Step 2 — dependencies
```bash
go get github.com/confluentinc/confluent-kafka-go/v2/kafka
go get github.com/jmoiron/sqlx github.com/lib/pq github.com/sirupsen/logrus
```

### Step 3 — Kafka KRaft (docker)
ใช้ `kafka.yaml` (KRaft mode — ดูไฟล์จริงในโปรเจกต์) แล้ว:
```bash
docker-compose -f kafka.yaml up -d   # kafdrop ดูที่ http://localhost:9090
```

### Step 4 — PostgreSQL + ตาราง
```sql
CREATE TABLE IF NOT EXISTS events (
    event_id   TEXT PRIMARY KEY,
    event_type TEXT,
    timestamp  TIMESTAMPTZ
);
```

### Step 5 — เขียนโค้ดตามลำดับ
1. `shared/kafka-config.go` (topic, group, strategy=cooperative-sticky, NumPartitions=4)
2. `repo/db.go` + `event-repo.go` + `repo-err.go` (blank import `_ "github.com/lib/pq"`)
3. `shared/types.go`
4. `producer/producer.go`
5. `consumer/parition-state.go` → `consumer/consumer.go`
6. `cmd/main.go` (เรียก `RunConsumer()`)

### Step 6 — รัน
```bash
go run ./cmd          # หรือ make app
```
ลองเปิด 2 terminal พร้อมกันเพื่อทดสอบ rebalance — จะเห็น log `✅ Assigned` / `❌ Revoking`

---

## 7. ข้อควรระวัง / Known issues

**สิ่งที่แก้ไปแล้ว** (จาก rebalance refactor): per-partition state ✓, state เป็น enum ✓, multi-partition ✓, regex topic ถอดออก ✓, `topic` field ตั้งถูก ✓, deadlock ใน commit loop หายไป (commit loop ย้ายไป PartitionState ไม่มี lock ค้างแล้ว) ✓, `lastCommited = offset-1` ยืนยันว่า**ตั้งใจ** (committed offset = ตัวถัดไปที่จะอ่าน) ✓

**ที่ยังเหลือ:**

1. **ยังไม่มี graceful shutdown** — `RunConsumer` block ที่ `<-exitCH` แต่ไม่มี signal handler (SIGINT/SIGTERM) → กด Ctrl+C ตอนนี้ producer ไม่ Flush, commit loop อาจถูกตัดกลางคัน
2. **`Event.Timespamp`** — สะกดผิด (ควร Timestamp) แต่ db tag ถูก เลยทำงานได้
3. **MsgCH block 5 วิ → drop เป็น Error** — `consumeLoop` ถ้า channel เต็มเกิน 5 วิ จะ drop message (`UpdateState Error`) — ป้องกัน deadlock แต่หมายถึง message นั้นจะถูก skip (ไม่ได้ process จริง) ควรมี retry/DLQ
4. **Replication factor = 1** — broker เดียว ตาย = ข้อมูลหาย (production ต้อง ≥3)
5. **ไม่มี retry / DLQ** — Error state แค่ปล่อยผ่าน commit ไม่มีการลองใหม่หรือเก็บไว้ที่อื่น
6. **DB credential hardcode ใน `db.go`** — ต้องย้ายไป env/secret ก่อน production

---

## 8. Scaling to production

### 8.1 กฎ partition + consumer group (5 ข้อ)

```
1. แต่ละ partition เป็นเจ้าของ offset ของตัวเอง และเรียงลำดับภายใน
2. 1 partition = 1 consumer ต่อ group  (ห้ามแชร์ partition ในกลุ่มเดียว)
3. 1 consumer ถือได้หลาย partition
4. ถ้า consumer > partition → ตัวที่เกินจะ IDLE (รอ rebalance)
5. คนละ consumer group อ่าน partition เดียวกันได้อิสระ (ต่างคนต่างจำ offset)
```

### 8.2 สถานะปัจจุบัน — implement ไปถึงไหนแล้ว

| ความสามารถ | สถานะ |
|---|---|
| Per-partition state | ✅ `PartitionState` ต่อ partition |
| Multi-partition (4) | ✅ `NumPartitions: 4` |
| Rebalance (assign/revoke) | ✅ `rebalanceCB` + commit-before-revoke |
| Cooperative-sticky | ✅ incremental assign/unassign |
| รันหลาย instance | ✅ เปิดหลาย process แล้ว Kafka แจก partition ให้ |
| Multi-broker / HA | ❌ ยัง 1 broker (RF=1) |
| DLQ / retry | ❌ ยังไม่มี |

โค้ดตอนนี้ scale ระดับ "หลาย consumer instance" ได้แล้ว (ดูรายละเอียด rebalance ใน `REBALANCE.md`) ที่เหลือคือ infra (multi-broker) + resilience (DLQ/retry) ตาม `LEARNING_ROADMAP.md` Phase 2-3

### 8.3 Scale in / Scale out (k8s ↔ Kafka)

**Scale out** = เพิ่ม instance (k8s เพิ่ม replicas) · **Scale in** = ลด instance (ลด replicas)

ทั้ง 3 ส่วน scale ไม่เหมือนกัน:

| ส่วน | scale ยังไง | = k8s replicas? | เกี่ยว rebalance? |
|---|---|---|---|
| **Producer** | เพิ่ม pod กี่ตัวก็ได้ ทุกตัวยิงเข้า topic เดียวกัน | ✅ ใช่ | ❌ ไม่ (producer ไม่มี group) |
| **Consumer** | เพิ่ม/ลด pod → Kafka แจก partition ใหม่ | ✅ ใช่ | ✅ ใช่ (หัวใจ) |
| **Partition** | แก้ config ที่ Kafka (เพิ่มได้/ลดไม่ได้) | ❌ **ไม่ใช่** | เป็น "เพดาน" |

**สำคัญ: Partition ≠ k8s replicas** — partition คือ config ของ topic ฝั่ง Kafka ไม่ได้ผูกกับ pod มันคือ **"จำนวนเลน" ตายตัว = เพดานของ consumer ที่ขนานได้**

```
topic มี 4 partition
 → scale consumer ได้สูงสุด 4 pod (ทำงานจริง)
 → pod ที่ 5, 6... = IDLE (กฎข้อ 4)
```

อยากขยายเพดาน = เพิ่ม partition ที่ Kafka (ไม่ใช่เพิ่ม pod)

**Scale in/out อยู่ตรงไหนในโค้ด** — ที่ rebalance callback:

```
Scale OUT (k8s เพิ่ม pod)          Scale IN (k8s ลด pod)
   ↓                                  ↓
consumer ใหม่ join group            consumer หาย/ถูกฆ่า
   ↓                                  ↓
Kafka rebalance                     Kafka rebalance
   ↓                                  ↓
assignPrntCB()                      revokePrtnCB()
→ pod เดิมคืน partition บางส่วน       → commit ค้างก่อนปล่อย ✅
→ pod ใหม่รับ partition ไป           → แจก partition ให้ตัวที่เหลือ
```

`revokePrtnCB` ที่ commit ก่อนปล่อย คือพระเอกตอน scale in — กัน pod ที่รับช่วงอ่านซ้ำเยอะ

**ภาพรวม k8s ↔ Kafka:**

```
API traffic เยอะขึ้น
   ↓ k8s HPA เพิ่ม consumer replicas (2 → 4 pod)     ← "API Scales"
   ↓ 4 pod join group "local_cg1"
   ↓ Kafka แจก 4 partition ให้ 4 pod → rebalance assign
   ↓ throughput รวมเพิ่ม (แต่ละ pod รับ 1 partition)
```

สรุป: **producer + consumer scale ตาม k8s replicas ✅ แต่ partition เป็นเพดานฝั่ง Kafka ไม่ scale ตาม pod** — scale out/in ที่เห็นผลจริงคือฝั่ง consumer ซึ่ง trigger rebalance

### 8.4 ทดสอบ rebalance

เปิด `go run ./cmd` หลาย terminal พร้อมกัน (group เดียวกัน) → Kafka จะแจก 4 partition ให้แต่ละ instance → ดู log `✅ Assigned partition` / `❌ Revoking partition` ถ้าปิดตัวนึง partition จะถูกแจกใหม่ให้ตัวที่เหลือ (rebalance)
