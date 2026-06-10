# golang_kafka — Code Walkthrough (ไล่ทุกบรรทัด)

อธิบายโค้ด **ทุกบรรทัด** — struct, func, method, pointer, variable — เรียงตาม **ลำดับการสร้างจริง** (build order)

> เวอร์ชันนี้ตรงกับโค้ดปัจจุบัน (มี rebalance + per-partition state + enum) อ่านคู่กับ `REBALANCE.md` สำหรับ rebalance เชิงลึก

---

## Go primer — syntax ที่ใช้ในโปรเจกต์นี้

| syntax | หมายถึง |
|---|---|
| `type X struct {…}` | นิยาม struct |
| `func (x *X) M()` | method ของ X — `(x *X)` คือ receiver (เหมือน `this`) |
| `*X` (pointer) / `&x` | ตัวชี้ไปที่อยู่ / เอาที่อยู่ของ x |
| `a, err := f()` | รับหลายค่า + `:=` ประกาศ+assign |
| `if err != nil {…}` | เช็ค error มาตรฐาน (ไม่มี try/catch) |
| `chan T` / `chan<- T` | channel / channel ส่งออกอย่างเดียว |
| `go f()` | รัน f() เป็น goroutine (ขนาน) |
| `defer f()` | เลื่อน f() ไปทำตอนฟังก์ชันจบ |
| `_ "pkg"` | blank import (โหลดเพื่อ `init()`) |
| `func F[T any](…)` | generic — T เป็น type parameter |
| `const ( A = iota; B; C )` | enum อัตโนมัติ (A=0, B=1, C=2) |
| `atomic.Int32` | ตัวเลขที่อ่าน/เขียนข้าม goroutine ได้ปลอดภัยโดยไม่ต้อง lock |
| `context.WithCancel` | สร้าง context ที่สั่งยกเลิกได้ (ใช้หยุด goroutine) |
| `switch v := e.(type)` | type switch — เช็คชนิดจริงของ interface |

---

## Build order (9 step)

```
Step 1  go mod init
Step 2  internal/shared/kafka-config.go      ← config (ไม่พึ่งใคร)
Step 3  internal/repo/db.go                  ← เชื่อม DB + util
Step 4  internal/repo/event-repo.go          ← Event + CRUD + TxClosure
Step 5  internal/repo/repo-err.go            ← ตรวจ duplicate key  ★ใหม่
Step 6  internal/shared/types.go             ← Message (พึ่ง repo.Event)
Step 7  internal/producer/producer.go        ← producer
Step 8  internal/consumer/parition-state.go  ← PartitionState + commit loop  ★ใหม่
Step 9  internal/consumer/consumer.go        ← consumer + rebalance
Step 10 cmd/main.go                          ← ประกอบทุกอย่าง
```

---

## Step 1 — `go mod init`

```bash
go mod init github.com/golang_kafka
```
ตั้งชื่อ module = prefix ของทุก import

---

## Step 2 — `internal/shared/kafka-config.go`

```go
type KafkaPartitionStrategies string
const (
    CooperativeStickyStrategy KafkaPartitionStrategies = "cooperative-sticky"
    RoundRobin                KafkaPartitionStrategies = "roundrobin"
)
```
- นิยาม type ใหม่จาก string + 2 ค่าคงที่ของกลยุทธ์แจก partition (ใช้ตอน rebalance)

```go
type KafkaConfig struct {
    DefaultTopic             string                    // "local_topic_sticky1"
    Host                     string                    // "localhost"
    ConsumerGroup            string                    // "local_cg1"
    ParititionAssignStrategy KafkaPartitionStrategies  // cooperative-sticky
    NumPartitions            int                       // 4
}

func NewKafkaConfig() *KafkaConfig {
    return &KafkaConfig{
        ParititionAssignStrategy: CooperativeStickyStrategy,
        DefaultTopic:             "local_topic_sticky1",
        Host:                     "localhost",
        ConsumerGroup:            "local_cg1",
        NumPartitions:            4,
    }
}
```
- config กลาง — เพิ่ม `NumPartitions: 4` (เปิดให้ขนานระดับ partition) + `strategy` (วิธี rebalance)
- คืน `*KafkaConfig` (pointer)

---

## Step 3 — `internal/repo/db.go`

```go
import (
    "math/rand"; "time"
    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"   // blank import: register driver "postgres"
)
```
- `_ "github.com/lib/pq"` — โหลด driver เพื่อให้ `init()` register ชื่อ "postgres" ไม่งั้น `Connect` panic

```go
func getDBConnString() string {
    return "host=localhost port=5433 user=alphamech password=... dbname=kafka_yt sslmode=disable"
}
func NewDBConn() (*sqlx.DB, error) {
    db, err := sqlx.Connect("postgres", getDBConnString())
    if err != nil { return nil, err }
    return db, nil
}
```
- `NewDBConn` เปิด connection + ping ทดสอบ คืน `(*sqlx.DB, error)`
- ⚠️ password hardcode — ควรย้ายไป env (ดู ARCHITECTURE §7)

```go
var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
var charset = "abc...XYZ0123456789"
func GenerateRandomString(length int) string { /* สุ่มตัวอักษรจาก charset */ }
```
- util สร้าง string สุ่ม (ใช้ทำ event id + consumer id)

---

## Step 4 — `internal/repo/event-repo.go`

```go
type Event struct {
    EventId   string    `db:"event_id"`
    EventName string    `db:"event_type"`
    Timespamp time.Time `db:"timestamp"`
}
```
- 1 แถวในตาราง `events`, `` `db:"..."` `` = struct tag บอก sqlx ว่า field ตรงกับ column ไหน

```go
func NewEvent() *Event { /* id สุ่ม 15 ตัว, name="test_event", time=now */ }

type EventRepo struct { repo *sqlx.DB; tableName string }
func NewEventRepo(db *sqlx.DB) *EventRepo { … }   // ฉีด DB เข้ามา (DI)

func (r *EventRepo) Insert(ctx, tx *sqlx.Tx, e *Event) (string, error) { … }  // INSERT ผ่าน tx
func (r *EventRepo) Get(ctx, tx *sqlx.Tx, eventID string) *Event { … }        // SELECT, ไม่เจอคืน nil
```
- ทุก method รับ `tx *sqlx.Tx` (transaction) → หลาย operation อยู่ใน tx เดียวได้
- `Get` คืน nil เมื่อ `sql.ErrNoRows` → ใช้เช็ค idempotent

```go
func TxClosure[T any](ctx, r *EventRepo, fn func(ctx, tx) (T, error)) (T, error) {
    tx, _ := r.repo.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
    defer func() {
        if r := recover(); r != nil { tx.Rollback(); panic(r) }  // panic → rollback
        if err != nil { tx.Rollback(); return }                  // error → rollback
        err = tx.Commit()                                         // สำเร็จ → commit
    }()
    res, err := fn(ctx, tx)
    return res, err
}
```
- generic helper ครอบ transaction — ส่ง closure (logic) เข้าไป ส่วน begin/commit/rollback จัดการให้

---

## Step 5 — `internal/repo/repo-err.go` ★

```go
var (
    ErrDuplicateCode = "23505"               // PostgreSQL: unique_violation
    ErrDuplicateMsg  = "duplicate key violation"
)

func IsDuplicateKeyErr(err error) bool {
    var pgErr *pq.Error
    if err != nil {
        if errors.As(err, &pgErr) {           // แปลง error เป็น *pq.Error
            return pgErr.Code == pq.ErrorCode(ErrDuplicateCode)
        }
    }
    return false
}
```
- ตรวจว่า error จาก insert เป็น "duplicate key" (PG code `23505`) ไหม
- `errors.As(err, &pgErr)` — เช็คว่า error chain มี `*pq.Error` ไหม ถ้ามีก็ดึงออกมา
- ใช้ทำ idempotent แบบพึ่ง DB constraint (insert ตรงๆ ถ้าซ้ำจับ error นี้) — ทางเลือกแทนการ `Get` ก่อน

---

## Step 6 — `internal/shared/types.go`

```go
type Message struct {
    Metadata *kafka.TopicPartition  // topic/partition/offset
    Event    *repo.Event            // เนื้อหา
}
func NewMessage(metadata *kafka.TopicPartition, data []byte) *Message {
    e := &repo.Event{}
    if err := json.Unmarshal(data, e); err != nil {
        panic(...)
    }
    return &Message{Metadata: metadata, Event: e}
}
```
- มัด metadata (ใช้ตอน commit + UpdateState — ต้องรู้ partition+offset) + event ที่ unmarshal แล้ว
- `Metadata` สำคัญขึ้นเพราะตอนนี้ส่งเข้า `UpdateState(tp, state)` เพื่อระบุว่า offset ไหนของ partition ไหน

---

## Step 7 — `internal/producer/producer.go`

```go
type KafkaProducer struct { producer *kafka.Producer; topic string }

func NewKafkaProducer() *KafkaProducer {        // ไม่รับ arg แล้ว
    cfg := shared.NewKafkaConfig()
    topic := cfg.DefaultTopic
    p, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})

    go func() {                                  // delivery report listener
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil { /* log fail */ }
                else { logrus...Info("Delivered message") }  // log partition+offset
            }
        }
    }()
    return &KafkaProducer{producer: p, topic: topic}
}

func (p *KafkaProducer) Produce(msg []byte) {    // ชื่อ Produce, รับ []byte
    cfg := shared.NewKafkaConfig()
    topic := cfg.DefaultTopic
    p.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value:          msg,
    }, nil)
}
```
- `NewKafkaProducer()` ไม่รับ arg (ดึง topic จาก config), เปิด goroutine ฟัง delivery report
- `Produce(msg []byte)` — `PartitionAny` ให้ Kafka กระจาย message ไปทั้ง 4 partition เอง
- (เดิมชื่อ `Producer(string)` → เปลี่ยนเป็น `Produce([]byte)`)

---

## Step 8 — `internal/consumer/parition-state.go` ★

state + commit loop **แยกต่อ partition**

```go
type MsgState = int32
const (
    MsgState_Pending MsgState = iota   // 0
    MsgState_Success                   // 1
    MsgState_Error                     // 2
)
```
- enum 3 สถานะ (เดิมเป็น `bool`) — แยก Error ออกจาก Success ได้

```go
type PartitionState struct {
    ID    int32
    Topic *string
    mu    *sync.RWMutex
    state map[kafka.Offset]MsgState    // offset → สถานะ ของ partition นี้

    maxReceived  *atomic.Int32         // offset สูงสุดที่อ่านมา
    lastCommited *atomic.Int32         // commit ถึงไหนแล้ว

    ctx    context.Context             // ใช้สั่งหยุด commit loop
    cancel context.CancelFunc
    exitCH chan struct{}               // ยืนยันว่า loop หยุดจริง
}
```
- **1 instance ต่อ 1 partition** — เพราะ offset แต่ละ partition นับแยก ใช้ map รวมจะชนกัน
- `atomic.Int32` — `maxReceived`/`lastCommited` อ่าน/เขียนข้าม goroutine ปลอดภัยโดยไม่ต้อง lock
- `ctx/cancel/exitCH` — กลไกหยุด loop ตอน partition ถูก revoke

```go
func NewPartitionState(tp *kafka.TopicPartition) *PartitionState {
    ctx, cancel := context.WithCancel(context.Background())
    initialLastCommited := tp.Offset - 1                      // committed = ตัวถัดไป → ล่าสุด = offset-1
    if tp.Offset == kafka.OffsetBeginning || tp.Offset < 0 {
        initialLastCommited = -1                              // ยังไม่เคย commit
    }
    ...store ใน atomic...
}
```
- เริ่ม `lastCommited = offset - 1` — **นี่ตอบเรื่อง offset-1 ที่เคยสงสัย** มันตั้งใจ (committed offset = "ตัวถัดไปที่จะอ่าน")

```go
func (ps *PartitionState) commitOffsetLoop(commitDur time.Duration, c *KafkaConsumer) {
    ticker := time.NewTicker(commitDur)
    defer func() { close(ps.exitCH); ticker.Stop() }()       // ตอนจบ: ปิด exitCH ยืนยันหยุด
    for {
        select {
        case <-ticker.C:                                      // ครบ 10 วิ
            select { case <-ps.ctx.Done(): return; default: } // เช็คถูกสั่งหยุดยัง
            latestToCommit, err := ps.findLatestToCommit()
            if err != nil { continue }
            c.consumer.CommitOffsets([]kafka.TopicPartition{{Topic: ps.Topic, Partition: ps.ID, Offset: latestToCommit}})
            ps.lastCommited.Store(int32(latestToCommit))
        case <-ps.ctx.Done():                                 // ถูกสั่งหยุด (revoke)
            return
        }
    }
}
```
- commit loop **ของแต่ละ partition** (ไม่ใช่รวม) — ticker ทุก 10 วิ → หา offset ที่ commit ได้ → commit
- `select { case <-ps.ctx.Done() }` — ออกจาก loop ทันทีเมื่อถูกสั่งหยุด (ตอน revoke)

```go
func (ps *PartitionState) findLatestToCommit() (kafka.Offset, error) {
    lastCommited := kafka.Offset(ps.lastCommited.Load())
    latestToCommit := kafka.Offset(ps.maxReceived.Load())
    if lastCommited == latestToCommit { return -1, err }       // ไม่มีอะไรใหม่ → ข้าม

    ps.mu.Lock(); defer ps.mu.Unlock()
    for offset := lastCommited; offset <= latestToCommit; offset++ {
        msgState, exists := ps.state[offset]
        if !exists { continue }
        if msgState != MsgState_Pending {                      // Success/Error → ผ่านได้
            delete(ps.state, offset)
            if len(ps.state) == 0 { latestToCommit = offset + 1; break }
            continue
        }
        latestToCommit = offset                                // เจอ Pending → หยุด (กำแพง)
        break
    }
    return latestToCommit, nil
}
```
- หัวใจ **sequential commit** — scan จาก lastCommited เจอ Pending = หยุด ส่วน Success/Error ลบทิ้งแล้วผ่าน
- ใช้ `Lock` แค่ตอน scan map (ไม่ค้าง lock ข้าม commit) — ต่างจากเวอร์ชันเก่าที่เสี่ยง deadlock

---

## Step 9 — `internal/consumer/consumer.go`

### 9.1 struct

```go
type KafkaConsumer struct {
    consumer     *kafka.Consumer
    ID           string                    // id สุ่ม (ดูง่ายตอนรันหลาย instance)
    topic        string
    IsReady      bool
    ReadyCH      chan struct{}
    MsgCH        chan *shared.Message      // ส่ง message ออกให้ handler
    exitCH       chan struct{}
    mu           *sync.RWMutex
    msgsStateMap map[int32]*PartitionState // partition → state (แยกต่อ partition!)
    commitDur    time.Duration             // 10s
    cfg          *shared.KafkaConfig
}
```
- `msgsStateMap map[int32]*PartitionState` — เปลี่ยนจาก `map[Offset]bool` เป็น **map[partition]→PartitionState** (เก็บ state แยกต่อ partition)

### 9.2 constructor

```go
c, _ := kafka.NewConsumer(&kafka.ConfigMap{
    "enable.auto.commit":              false,
    "auto.offset.reset":               "earliest",
    "go.application.rebalance.enable": true,                          // จัดการ rebalance เอง
    "partition.assignment.strategy":   string(cfg.ParititionAssignStrategy),  // cooperative-sticky
})
...
c.SubscribeTopics([]string{consumer.topic}, consumer.rebalanceCB)    // ผูก rebalance callback
return consumer
```
- เพิ่ม config `go.application.rebalance.enable` + strategy
- subscribe พร้อม **rebalance callback** (arg ที่ 2) — ไม่มี regex topic แล้ว
- คืน `*KafkaConsumer` ตัวเดียว (เดิมคืน error ด้วย — เปลี่ยนแล้ว)

### 9.3 RunConsumer — ตัวเริ่ม (ต้องเรียกจาก main)

```go
func (c *KafkaConsumer) RunConsumer() struct{} {
    go c.checkReadyToAccept()
    go c.consumeLoop()
    return <-c.exitCH        // block จนกว่า consumeLoop จะปิด exitCH
}
```
- เดิม constructor เปิด goroutine ให้ ตอนนี้ย้ายมา `RunConsumer` → **main ต้องเรียก `go s.consumer.RunConsumer()`**

### 9.4 consumeLoop — อ่าน message

```go
for {
    msg, err := c.consumer.ReadMessage(time.Second)
    if err != nil && err.(kafka.Error).IsTimeout() { continue }     // timeout = ปกติ
    if err != nil && !err.(kafka.Error).IsTimeout() { /* log */ continue }
    if msg == nil { continue }

    if firstMsg { close(c.ReadyCH) }                                // message แรก = พร้อม
    firstMsg = false

    c.appendMsgState(&msg.TopicPartition)                           // จด Pending (per-partition)
    msgRequest := shared.NewMessage(&msg.TopicPartition, msg.Value)
    select {
    case c.MsgCH <- msgRequest:                                     // ส่งเข้า channel
    case <-time.After(5 * time.Second):                             // ถ้า channel เต็มเกิน 5 วิ
        c.UpdateState(&msg.TopicPartition, MsgState_Error)          // → drop เป็น Error (กัน block)
    }
}
```
- `select` + `time.After(5s)` — กัน consumer ค้างถ้า handler ช้า/channel เต็ม (แต่ message นั้นจะถูก skip → ดู known issue)

### 9.5 appendMsgState / UpdateState

```go
func (c *KafkaConsumer) appendMsgState(tp *kafka.TopicPartition) {
    c.mu.RLock(); prtnState := c.msgsStateMap[tp.Partition]; c.mu.RUnlock()
    prtnState.mu.Lock(); defer prtnState.mu.Unlock()
    prtnState.state[tp.Offset] = MsgState_Pending                   // จด Pending ใน partition นั้น
    if prtnState.maxReceived.Load() < int32(tp.Offset) {
        prtnState.maxReceived.Store(int32(tp.Offset))
    }
}

func (c *KafkaConsumer) UpdateState(tp *kafka.TopicPartition, newState MsgState) {
    c.mu.Lock(); prtnState, ok := c.msgsStateMap[tp.Partition]; c.mu.Unlock()
    if !ok { return }
    prtnState.mu.Lock(); prtnState.state[tp.Offset] = newState; prtnState.mu.Unlock()
}
```
- 2 ชั้นของ lock: `c.mu` กัน `msgsStateMap` (หา partition), `prtnState.mu` กัน `state` ของ partition นั้น
- `UpdateState` — handler เรียกหลัง process เสร็จ ตั้ง Success/Error

### 9.6 rebalance callbacks (สรุป — เต็มใน REBALANCE.md)

```go
func (c *KafkaConsumer) rebalanceCB(_ *kafka.Consumer, event kafka.Event) error {
    switch ev := event.(type) {
    case kafka.AssignedPartitions: return c.assignPrntCB(&ev)   // ได้ partition → สร้าง state + เปิด commit loop
    case kafka.RevokedPartitions:  return c.revokePrtnCB(&ev)   // เสีย partition → commit ค้าง + หยุด loop + ปล่อย
    }
    return nil
}
```
- `assignPrntCB` — ถาม committed offset → `NewPartitionState` ต่อ partition → `go commitOffsetLoop` → `IncrementalAssign`
- `revokePrtnCB` — `cancel()` loop → `findLatestToCommit` → **`CommitOffsets` ก่อนปล่อย** → `IncrementalUnassign`

---

## Step 10 — `cmd/main.go`

```go
type Server struct {
    producer  *producer.KafkaProducer
    consumer  *consumer.KafkaConsumer
    msgCH     chan *shared.Message
    eventRepo *repo.EventRepo
}

func NewServer(eventRepo *repo.EventRepo) *Server {
    msgCH := make(chan *shared.Message, 64)        // buffered channel
    c := consumer.NewKafkaConsumer(msgCH)          // คืนค่าเดียว
    return &Server{
        producer:  producer.NewKafkaProducer(),    // ไม่รับ arg
        consumer:  c, msgCH: msgCH, eventRepo: eventRepo,
    }
}

func (s *Server) produceMsg() {
    ticket := time.NewTicker(time.Second)
    for range ticket.C {
        event := repo.NewEvent()
        payload, _ := json.Marshal(event)
        s.producer.Produce(payload)                 // ส่ง []byte ตรงๆ
    }
}

func (s *Server) saveToDB(ctx, msg *shared.Message) {
    _, err := repo.TxClosure(ctx, s.eventRepo, func(ctx, tx) (string, error) {
        if s.eventRepo.Get(ctx, tx, msg.Event.EventId) != nil {
            return "", nil                          // เคยมี = idempotent skip (ถือว่าสำเร็จ)
        }
        return s.eventRepo.Insert(ctx, tx, msg.Event)
    })
    if err != nil {
        s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Error)   // พลาด → Error
        return
    }
    s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)     // สำเร็จ → Success
}

func main() {
    db, _ := repo.NewDBConn()
    s := NewServer(repo.NewEventRepo(db))

    go s.consumer.RunConsumer()        // ★ เริ่ม consumer (เดิม constructor ทำ)
    go s.produceMsg()
    for msg := range s.msgCH {
        go s.handleMsg(msg)            // fan-out: process ขนาน
    }
}
```
- `RunConsumer()` ต้องเรียกเอง (ย้ายมาจาก constructor)
- `saveToDB` ส่งผลจริงเข้า `UpdateState` (Success/Error) แทนการ mark complete เฉยๆ → enum Error ทำงานตามออกแบบ
- `Produce(payload)` ส่ง `[]byte` ตรง (ไม่ต้อง `string()`)

---

## สรุปการประกอบ + goroutine ที่วิ่งพร้อมกัน

```
main()
 ├─ NewDBConn → NewEventRepo → NewServer
 │                              ├─ NewKafkaConsumer (subscribe + rebalanceCB)
 │                              └─ NewKafkaProducer (delivery report goroutine)
 ├─ go RunConsumer()  → checkReadyToAccept + consumeLoop
 ├─ go produceMsg()   → ยิง event ทุกวิ
 └─ for range msgCH   → go handleMsg → saveToDB → UpdateState

goroutine ตอนรัน:
1. produceMsg                       5. checkReadyToAccept
2. producer delivery-report         6. main loop + handleMsg แต่ละตัว
3. consumeLoop (อ่าน)               7. commitOffsetLoop — 1 ตัว/partition (เพิ่ม/ลดตาม rebalance)
4. rebalanceCB (ตอน assign/revoke)
```

เชื่อมกันด้วย `MsgCH` (channel) + `msgsStateMap` (per-partition state + mutex/atomic)

> อ่านคู่: `ARCHITECTURE.md` (ภาพรวม+scaling), `MESSAGE_LIFECYCLE.md` (flow), `REBALANCE.md` (rebalance เชิงลึก)


