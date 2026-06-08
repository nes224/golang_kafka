# golang_kafka — Code Walkthrough (ไล่ทุกบรรทัด)

เอกสารนี้อธิบายโค้ด **ทุกบรรทัด** — ทุก struct, func, method, pointer, variable — เรียงตาม **ลำดับการสร้างจริง** (build order) เพื่อให้ทั้งเข้าใจโค้ดและสร้างขึ้นใหม่ได้ทีละ step

> วิธีอ่าน: ไล่จากบนลงล่างได้เลย แต่ละไฟล์เรียงตาม dependency — ไฟล์ที่ไม่พึ่งใครมาก่อน ไฟล์ที่ประกอบทุกอย่าง (`main.go`) มาท้ายสุด ถ้าสร้างใหม่ก็พิมพ์ตามลำดับนี้

---

## Go primer — syntax ที่ใช้ในโปรเจกต์นี้

ปูพื้น 10 อย่างที่เจอซ้ำๆ ก่อน จะได้ไม่ต้องอธิบายซ้ำทุกจุด

| syntax | หมายถึง |
|---|---|
| `type X struct {…}` | นิยาม struct (โครงสร้างข้อมูล รวมหลาย field) |
| `func (x *X) M()` | **method** ของ type X — `(x *X)` คือ receiver (เหมือน `this`) |
| `*X` (pointer) | ตัวชี้ไปยังที่อยู่ของ X — แก้ค่าจริงได้ + ไม่ copy ทั้งก้อน |
| `&x` | เอา "ที่อยู่" ของ x (สร้าง pointer ชี้ไป x) |
| `a, err := f()` | รับค่าหลายตัว (Go คืนได้หลายค่า) + `:=` ประกาศ+assign ในที |
| `if err != nil {…}` | สำนวนเช็ค error มาตรฐาน (Go ไม่มี try/catch) |
| `chan T` | channel — ท่อส่งข้อมูล T ข้าม goroutine |
| `go f()` | สั่งรัน f() เป็น goroutine (ขนาน ไม่รอ) |
| `defer f()` | เลื่อน f() ไปทำตอนฟังก์ชันปัจจุบันจบ |
| `_ "pkg"` | blank import — โหลด pkg เพื่อ side-effect (`init()`) โดยไม่เรียกตรงๆ |
| `func F[T any](…)` | generic — T เป็น type parameter (ใส่ type อะไรก็ได้) |

---

## Build order (ภาพรวม 8 step)

```
Step 1  go mod init                         ← ตั้ง module
Step 2  internal/shared/kafka-config.go     ← config (ไม่พึ่งใคร)
Step 3  internal/repo/db.go                 ← เชื่อม DB + util
Step 4  internal/repo/event-repo.go         ← Event + CRUD + TxClosure
Step 5  internal/shared/types.go            ← Message (พึ่ง repo.Event)
Step 6  internal/producer/producer.go       ← producer (พึ่ง shared)
Step 7  internal/consumer/consumer.go       ← consumer (พึ่ง shared)
Step 8  cmd/main.go                          ← ประกอบทุกอย่าง
```

เหตุผลของลำดับ: ไฟล์ล่างพึ่งไฟล์บน เลยต้องมีของบนก่อน ถ้าพิมพ์สลับจะ compile ไม่ผ่านเพราะอ้างของที่ยังไม่มี

---

## Step 1 — `go mod init`

```bash
go mod init github.com/golang_kafka
```

สร้างไฟล์ `go.mod` ที่ประกาศชื่อ module = `github.com/golang_kafka` ชื่อนี้คือ prefix ของทุก import ในโปรเจกต์ (เช่น `github.com/golang_kafka/internal/repo`) ทุก `go get` หลังจากนี้จะถูกบันทึกใน go.mod

---

## Step 2 — `internal/shared/kafka-config.go`

ไฟล์เล็กสุด ไม่พึ่งใคร เลยสร้างก่อน

```go
package shared
```
ประกาศว่าไฟล์นี้อยู่ใน package `shared`

```go
type KafkaConfig struct {
    Topic         string   // ชื่อ topic ที่จะใช้ ("local_topic")
    ConsumerGroup string   // group.id ของ consumer ("local_cg")
    Host          string   // ที่อยู่ Kafka broker ("localhost")
}
```
- `type KafkaConfig struct` — นิยาม struct เก็บ config 3 ตัว
- ทั้ง 3 field เป็น `string`

```go
func NewKafkaConfig() *KafkaConfig {
    return &KafkaConfig{
        Topic:         "local_topic",
        ConsumerGroup: "local_cg",
        Host:          "localhost",
    }
}
```
- `func NewKafkaConfig() *KafkaConfig` — constructor คืน **pointer** ไปยัง KafkaConfig (`*KafkaConfig`)
- `return &KafkaConfig{…}` — `&` สร้าง instance แล้วคืน pointer ของมัน
- ทำไมคืน pointer: ไม่ต้อง copy struct + ฝั่งเรียกใช้อ้าง field เดียวกันได้ (ที่นี่จริงๆ struct เล็ก จะ value ก็ได้ แต่ pointer เป็นสำนวนนิยมของ constructor)

> นี่คือ pattern "constructor function" ของ Go — Go ไม่มี keyword `new`/class แบบภาษาอื่น เลยใช้ฟังก์ชัน `NewXxx()` คืน instance แทน

---

## Step 3 — `internal/repo/db.go`

เชื่อม PostgreSQL + ฟังก์ชัน util

```go
package repo

import (
    "math/rand"
    "time"

    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"   // blank import: register driver "postgres"
)
```
- `sqlx` — library ครอบ `database/sql` ให้ใช้ง่ายขึ้น (map struct ↔ row ได้)
- `_ "github.com/lib/pq"` — **blank import** ตัว driver PostgreSQL. เครื่องหมาย `_` แปลว่า "โหลด package นี้แต่ไม่เรียกใช้ตรงๆ" — โหลดเพื่อให้ `init()` ของ pq ทำงาน ซึ่งจะ **register driver ชื่อ "postgres"** เข้า `database/sql` ถ้าไม่มีบรรทัดนี้ → `sqlx.Connect("postgres", …)` จะ panic `unknown driver`

```go
func getDBConnString() string {
    return "host=localhost port=5433 user=alphamech password=alphamech1234@ dbname=kafka_yt sslmode=disable"
}
```
- ฟังก์ชันเล็ก คืน connection string (DSN) ของ PostgreSQL
- ตัวพิมพ์เล็กขึ้นต้น (`getDBConnString`) = **private** เห็นแค่ใน package `repo` (Go ใช้ตัวพิมพ์ใหญ่/เล็กคุม export: ใหญ่=public, เล็ก=private)

```go
func NewDBConn() (*sqlx.DB, error) {
    db, err := sqlx.Connect("postgres", getDBConnString())
    if err != nil {
        return nil, err
    }
    return db, nil
}
```
- `func NewDBConn() (*sqlx.DB, error)` — คืน 2 ค่า: pointer ไป `sqlx.DB` + error
- `sqlx.Connect("postgres", …)` — เปิด connection ด้วย driver ชื่อ "postgres" (ที่ pq register ไว้) + ping ทดสอบเลย
- `if err != nil { return nil, err }` — ถ้าต่อไม่ติด คืน nil + error ออกไป
- `return db, nil` — สำเร็จ คืน db + nil (ไม่มี error)

```go
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
```
- `var seededRand *rand.Rand` — ตัวแปร package-level (อยู่นอกฟังก์ชัน ใช้ได้ทั้งไฟล์) ชนิด pointer ไป `rand.Rand`
- `rand.New(rand.NewSource(time.Now().UnixNano()))` — สร้าง random generator ที่ seed ด้วยเวลาปัจจุบัน (ns) เพื่อให้สุ่มไม่ซ้ำทุกครั้งที่รัน
- `charset` — ชุดตัวอักษรที่ใช้สุ่ม

```go
func GenerateRandomString(length int) string {
    b := make([]byte, length)
    for i := range b {
        b[i] = charset[seededRand.Intn(len(charset))]
    }
    return string(b)
}
```
- `func GenerateRandomString(length int) string` — สร้าง string สุ่มยาว `length` (public เพราะขึ้นต้นตัวใหญ่)
- `b := make([]byte, length)` — สร้าง slice ของ byte ขนาด length (`make` ใช้สร้าง slice/map/channel)
- `for i := range b` — วน index ของ b
- `charset[seededRand.Intn(len(charset))]` — สุ่ม index ใน charset (`Intn(n)` = สุ่ม 0..n-1) แล้วหยิบตัวอักษรนั้น
- `return string(b)` — แปลง []byte กลับเป็น string

---

## Step 4 — `internal/repo/event-repo.go`

นิยาม Event + การอ่าน/เขียน DB + helper ครอบ transaction

```go
type Event struct {
    EventId   string    `db:"event_id"`
    EventName string    `db:"event_type"`
    Timespamp time.Time `db:"timestamp"`
}
```
- struct ที่แทน 1 แถวในตาราง `events`
- `` `db:"event_id"` `` คือ **struct tag** — บอก sqlx ว่า field `EventId` ตรงกับ column `event_id` ในตาราง (ใช้ตอน map row ↔ struct)
- หมายเหตุ: `Timespamp` สะกดผิด (ควร Timestamp) แต่ db tag ถูก เลยทำงานได้

```go
func NewEvent() *Event {
    id := GenerateRandomString(15)
    return &Event{
        EventId:   id,
        EventName: "test_event",
        Timespamp: time.Now(),
    }
}
```
- constructor สร้าง Event ใหม่: id สุ่ม 15 ตัว, ชื่อคงที่ "test_event", เวลาปัจจุบัน
- คืน `*Event` (pointer)

```go
type EventRepo struct {
    repo      *sqlx.DB   // connection pool
    tableName string     // "events"
}

func NewEventRepo(db *sqlx.DB) *EventRepo {
    return &EventRepo{repo: db, tableName: "events"}
}
```
- `EventRepo` ห่อ DB connection + ชื่อตาราง — เป็น "repository" รวมงาน DB ของ Event ไว้ที่เดียว
- constructor รับ `*sqlx.DB` (ฉีดเข้ามา = dependency injection) แล้วเก็บไว้

```go
func (r *EventRepo) Insert(ctx context.Context, tx *sqlx.Tx, e *Event) (string, error) {
    _, err := tx.NamedExecContext(ctx,
        fmt.Sprintf("INSERT INTO %s (event_type, event_id, timestamp) VALUES(:event_type, :event_id, :timestamp)", r.tableName), e)
    if err != nil {
        fmt.Printf("err on insert = %v\n", err)
        return "", err
    }
    return e.EventId, nil
}
```
- `func (r *EventRepo) Insert(...)` — **method** ของ EventRepo (receiver `r`)
- `ctx context.Context` — ส่ง context เข้าไป (ใช้ยกเลิก/timeout ได้)
- `tx *sqlx.Tx` — รับ **transaction** เข้ามา (ไม่ใช้ connection ตรงๆ → ทำให้หลาย operation อยู่ใน tx เดียวได้)
- `e *Event` — ข้อมูลที่จะ insert
- `tx.NamedExecContext` — รัน SQL แบบ named parameter (`:event_id` map กับ field ผ่าน db tag)
- `_, err :=` — ไม่สนค่าแรก (Result) เอาแค่ err
- คืน `(string, error)` — event id ที่ insert + error

```go
func (r *EventRepo) Get(ctx context.Context, tx *sqlx.Tx, eventID string) *Event {
    e := &Event{}
    q := fmt.Sprintf("SELECT event_id from %s WHERE event_id = $1", r.tableName)
    err := tx.GetContext(ctx, e, q, eventID)
    if err != nil {
        if err == sql.ErrNoRows {
            return nil
        }
    }
    return e
}
```
- ค้น event ตาม id — `$1` คือ placeholder ของ PostgreSQL (กัน SQL injection)
- `tx.GetContext(ctx, e, q, eventID)` — query แล้ว map ผลลง `e`
- `if err == sql.ErrNoRows { return nil }` — ถ้าไม่เจอแถว คืน nil (= ยังไม่มี event นี้) ← จุดนี้คือหัวใจของ idempotent
- คืน `*Event` (nil ถ้าไม่เจอ)

```go
func TxClosure[T any](ctx context.Context, r *EventRepo, fn func(ctx context.Context, tx *sqlx.Tx) (T, error)) (T, error) {
```
- `func TxClosure[T any](...)` — **generic function** — `[T any]` บอกว่า T เป็น type อะไรก็ได้ (return type ยืดหยุ่น)
- `fn func(ctx, tx) (T, error)` — รับ **ฟังก์ชัน(closure)** เข้ามาเป็น argument — นี่คือ "งานที่อยากทำใน transaction"
- ไอเดีย: คนเรียกแค่เขียน logic ส่งเข้ามา ส่วน begin/commit/rollback ให้ helper จัดการ

```go
    tx, err := r.repo.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
    if err != nil {
        panic("unable to start TX")
    }
```
- `BeginTxx` — เปิด transaction ใหม่, `Isolation: ReadCommitted` = ระดับการแยกของ tx (อ่านเห็นแต่ข้อมูลที่ commit แล้ว)
- เปิดไม่ได้ก็ panic

```go
    defer func() {
        if r := recover(); r != nil {   // ถ้ามี panic
            tx.Rollback()
            panic(r)                     // rollback แล้วโยน panic ต่อ
        }
        if err != nil {                  // ถ้ามี error
            tx.Rollback()
            return
        }
        err = tx.Commit()                // ไม่มีปัญหา → commit
        if err != nil {
            fmt.Printf("err on commit = %v\n", err)
        }
    }()
```
- `defer func(){…}()` — ฟังก์ชันนี้จะทำงาน **ตอน TxClosure จบ** (ไม่ว่าจบปกติหรือ panic)
- `recover()` — ดักจับ panic (ถ้ามี) → rollback แล้วโยนต่อ
- ถ้า `err != nil` → rollback
- ไม่งั้น → `tx.Commit()`
- รูปแบบนี้ทำให้ทุก exit path จัดการ tx ถูกต้องอัตโนมัติ

```go
    res, err := fn(ctx, tx)
    if err != nil {
        return res, err
    }
    return res, err
}
```
- เรียก closure ที่รับเข้ามา ส่ง tx ให้ → ได้ผล + error
- ค่า `err` ตรงนี้คือตัวเดียวกับที่ `defer` ด้านบนเช็ค (closure variable) → ตัดสินว่า commit หรือ rollback

---

## Step 5 — `internal/shared/types.go`

นิยามรูปแบบ message ที่ส่งผ่าน channel (พึ่ง `repo.Event` เลยต้องมาหลัง repo)

```go
type Message struct {
    Metadata *kafka.TopicPartition  // topic/partition/offset
    Event    *repo.Event            // เนื้อหา event ที่ unmarshal แล้ว
}
```
- มัด 2 อย่าง: **Metadata** (รู้ว่า message มาจาก offset ไหน — ใช้ตอน commit) + **Event** (ข้อมูลจริงไป process)
- ทั้งคู่เป็น pointer

```go
func NewMessage(metadata *kafka.TopicPartition, data []byte) *Message {
    e := &repo.Event{}
    err := json.Unmarshal(data, e)
    if err != nil {
        panic(fmt.Sprintf("err unmarshalling event = %v\n", err))
    }
    return &Message{Metadata: metadata, Event: e}
}
```
- รับ metadata + `data []byte` (ตัว value ดิบจาก Kafka)
- `json.Unmarshal(data, e)` — แปลง JSON bytes → struct Event (เก็บลง `e`)
- พังก็ panic — นี่คือจุดที่เคย error ตอน producer ส่ง plain text (ไม่ใช่ JSON)
- คืน `*Message` พร้อมใช้

---

## Step 6 — `internal/producer/producer.go`

```go
type KafkaProducer struct {
    producer *kafka.Producer  // ตัว producer จริงของ library
    topic    string           // topic ที่จะยิงเข้า
}
```
- ห่อ `*kafka.Producer` + จำ topic

```go
func NewKafkaProducer(topic string) *KafkaProducer {
    cfg := shared.NewKafkaConfig()
    if topic == "" {
        topic = cfg.Topic
    }
```
- ดึง config, ถ้าไม่ส่ง topic มา (`""`) ใช้ default จาก config

```go
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
    if err != nil {
        panic(err)
    }
    // ❌ ห้าม defer p.Close() ตรงนี้ — constructor return ปุ๊บ producer ปิดทันที
```
- สร้าง producer ชี้ไป broker, พังก็ panic
- คอมเมนต์เตือน bug ที่เคยเจอ: `defer p.Close()` ใน constructor = ปิดทันทีที่ฟังก์ชันจบ → ใช้ไม่ได้

```go
    go func() {
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
                } else {
                    fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
                }
            }
        }
    }()
```
- `go func(){…}()` — เปิด goroutine ฟัง **delivery report**
- `for e := range p.Events()` — วนรับ event จาก channel ของ producer
- `switch ev := e.(type)` — **type switch** เช็คว่า event เป็นชนิดไหน
- `case *kafka.Message:` — ถ้าเป็นรายงานผลส่ง message → log สำเร็จ/ล้มเหลว
- ทำไมต้องมี: `Produce()` เป็น **async** (return ทันที) ผลส่งจริงมาทีหลังทาง channel นี้

```go
    return &KafkaProducer{producer: p, topic: topic}
}
```
- คืน instance พร้อมใช้

```go
func (p *KafkaProducer) Producer(msg string) {
    err := p.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
        Value:          []byte(msg),
    }, nil)
    if err != nil {
        fmt.Printf("error producing msg := %v\n", err)
    }
}
```
- method ยิง message จริง
- `TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny}` — ส่งเข้า topic นี้, `PartitionAny` = ให้ Kafka เลือก partition เอง
- `Value: []byte(msg)` — payload (แปลง string → bytes)
- arg ที่ 2 เป็น `nil` = ไม่ส่ง delivery channel เฉพาะ (ใช้ channel กลางที่ goroutine ข้างบนฟังอยู่)

```go
func (p *KafkaProducer) Close() {
    p.producer.Flush(5000)   // รอ message ค้างใน queue ส่งให้หมด (สูงสุด 5 วิ)
    p.producer.Close()       // ปิด producer
}
```
- ไว้เรียกตอน shutdown — `Flush` กันข้อมูลค้างหาย

---

## Step 7 — `internal/consumer/consumer.go`

ไฟล์ใหญ่สุด หัวใจของ sequential commit

### 7.1 struct

```go
type KafkaConsumer struct {
    Consumer     *kafka.Consumer        // consumer จริงของ library (public)
    topic        string
    msgCH        chan<- *shared.Message  // channel "ส่งออกอย่างเดียว" (send-only)
    readyCH      chan struct{}           // สัญญาณว่าพร้อม
    exitCH       chan struct{}           // สัญญาณให้หยุด
    isReady      bool

    msgsStateMap map[kafka.Offset]bool   // offset → process เสร็จยัง
    lastCommited kafka.Offset            // commit ถึง offset ไหนแล้ว
    maxReceived  *kafka.TopicPartition   // offset สูงสุดที่อ่านมา
    mu           *sync.RWMutex           // ล็อกกัน race ตอนแตะ stateMap
    commitDur    time.Duration           // คาบเวลา commit (15 วิ)
}
```
- `chan<- *shared.Message` — เครื่องหมาย `<-` หลัง `chan` = channel ส่งออกอย่างเดียว (consumer ส่งเข้า ไม่อ่านออก)
- `chan struct{}` — channel ที่ไม่ส่งข้อมูลจริง ใช้แค่เป็น "สัญญาณ" (`struct{}` กินหน่วยความจำ 0)
- `map[kafka.Offset]bool` — map จาก offset → สถานะเสร็จ
- `*sync.RWMutex` — mutex แบบอ่าน/เขียน ป้องกัน map จากหลาย goroutine

### 7.2 constructor

```go
c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":  cfg.Host,
    "group.id":           cfg.ConsumerGroup,
    "enable.auto.commit": false,        // ← ปิด auto-commit คุมเอง
})
```
- สร้าง consumer, จุดสำคัญ `enable.auto.commit: false` — ไม่ให้ library commit เอง

```go
tp := kafka.TopicPartition{Topic: &cfg.Topic, Partition: 0}
commited, err := c.Committed([]kafka.TopicPartition{tp}, int(time.Second)*5)
latestComm := commited[len(commited)-1].Offset
logrus.WithField("OFFSET", latestComm).Info("starting POSITION")
```
- `tp` — ระบุ topic + partition 0
- `c.Committed(...)` — ถาม Kafka ว่า group นี้เคย commit ถึง offset ไหน (timeout 5 วิ)
- `commited[len(commited)-1].Offset` — เอา offset ตัวสุดท้าย = จุดเริ่มอ่านต่อ
- log บอกตำแหน่งเริ่ม

```go
maxReceived := &kafka.TopicPartition{Topic: tp.Topic, Partition: tp.Partition, Offset: latestComm}

consumer := &KafkaConsumer{
    Consumer: c, msgCH: msgCH,
    readyCH: make(chan struct{}), exitCH: make(chan struct{}),
    isReady: false, topic: cfg.ConsumerGroup,
    mu: new(sync.RWMutex), msgsStateMap: map[kafka.Offset]bool{},
    lastCommited: latestComm, maxReceived: maxReceived,
    commitDur: 15 * time.Second,
}
```
- เริ่มต้น `maxReceived` และ `lastCommited` ที่ offset เดิมที่เคย commit
- `make(chan struct{})` — สร้าง channel, `new(sync.RWMutex)` — สร้าง mutex, `map[...]{}` — สร้าง map ว่าง
- `commitDur: 15 * time.Second` — commit ทุก 15 วิ

```go
consumer.initializeKafkaTopic(cfg.Host, cfg.Topic)
err = c.SubscribeTopics([]string{cfg.Topic, "^aRegex.*[Tt]opic"}, nil)

go consumer.commitOffsetLoop()
go consumer.checkReadyToAccept()
go consumer.readMsgLoop()
return consumer, nil
```
- สร้าง topic ถ้ายังไม่มี → subscribe → เปิด **3 goroutine** (commit loop / ready check / read loop) → คืน consumer
- `"^aRegex.*[Tt]opic"` — regex topic ที่ไม่มีจริง (ทำให้ log รก — ลบได้)

### 7.3 readMsgLoop — อ่าน message

```go
func (c *KafkaConsumer) readMsgLoop() {
    defer c.Consumer.Close()
    for {
        msg, err := c.Consumer.ReadMessage(time.Second)
        if err != nil {
            if kerr, ok := err.(kafka.Error); ok && kerr.IsTimeout() {
                continue            // timeout = ปกติ ไม่มี message ช่วงนั้น
            }
            fmt.Printf("Consumer error: %v\n", err)
            continue
        }
        c.appendMsgState(&msg.TopicPartition)              // จด offset = false
        payload := shared.NewMessage(&msg.TopicPartition, msg.Value)  // unmarshal
        c.msgCH <- payload                                  // ส่งเข้า channel
    }
}
```
- `ReadMessage(time.Second)` — อ่าน message รอสูงสุด 1 วิ
- `err.(kafka.Error)` — **type assertion** แปลง error เป็น `kafka.Error` เพื่อเช็ค `.IsTimeout()`
- `ok` — บอกว่า assertion สำเร็จไหม (กัน panic ถ้า type ไม่ตรง)
- timeout → `continue` วนใหม่ (นี่คือ bug ที่แก้ไปแล้ว — เดิมไม่เช็คเลยทำ msg เป็น nil แล้ว panic)
- ถ้าได้ message → จด state, unmarshal, ส่งเข้า channel

### 7.4 appendMsgState / MarkAsComplete — จัดการ state (มี lock)

```go
func (c *KafkaConsumer) appendMsgState(tp *kafka.TopicPartition) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.msgsStateMap[tp.Offset] = false           // เพิ่งอ่าน ยังไม่ process
    if c.maxReceived.Offset < tp.Offset {
        c.maxReceived = &kafka.TopicPartition{Topic: tp.Topic, Partition: tp.Partition, Offset: tp.Offset}
    }
}
```
- `c.mu.Lock()` / `defer c.mu.Unlock()` — ล็อกก่อนแตะ map, ปลดตอนจบ
- จด offset ใหม่ = `false` (ยังไม่เสร็จ) + อัปเดต offset สูงสุดถ้ามากกว่าเดิม

```go
func (c *KafkaConsumer) MarkAsComplete(tp *kafka.TopicPartition) {
    logrus.WithField("OFFSET", tp.Offset).Info("MarkAsComplete")
    c.mu.Lock()
    defer c.mu.Unlock()
    c.msgsStateMap[tp.Offset] = true            // process เสร็จแล้ว
}
```
- เปลี่ยน state ของ offset เป็น `true` — เรียกจาก handler ตอน process เสร็จ (ผ่าน `defer` ใน saveToDB)

### 7.5 commitOffsetLoop — หัวใจ sequential commit

```go
ticker := time.NewTicker(c.commitDur)        // เต้นทุก 15 วิ
for {
    select {
    case <-ticker.C:                          // ถึงรอบ commit
        c.mu.Lock()
        ...
        for offset := c.lastCommited; offset < c.maxReceived.Offset; offset++ {
            completed, exists := c.msgsStateMap[offset]
            if !exists { continue }
            if completed {
                delete(c.msgsStateMap, offset)    // เสร็จแล้ว เก็บกวาด
                continue
            }
            latestToCommit.Offset = offset        // เจอตัวยังไม่เสร็จ → หยุด
            break
        }
        c.mu.Unlock()
        ...
        c.Consumer.CommitOffsets([]kafka.TopicPartition{latestToCommit})
    case <-c.exitCH:
        return                                 // ได้สัญญาณหยุด → ออก
    }
}
```
- `select { case <-ticker.C: … case <-c.exitCH: … }` — รอสัญญาณจาก 2 channel: ครบ 15 วิ หรือ สั่งหยุด
- loop ไล่จาก `lastCommited` ขึ้นไปหา offset แรกที่ยัง `false` → commit ได้แค่ถึงก่อนหน้านั้น (sequential — ข้ามรูไม่ได้)
- `delete(c.msgsStateMap, offset)` — ตัวที่เสร็จและจะ commit แล้วก็ลบทิ้งจาก map (ไม่ให้ map โตไม่หยุด)
- `CommitOffsets(...)` — commit จริงเข้า Kafka

> ⚠️ จุดนี้มี known issue: บาง path `continue`/`break` ขณะถือ lock ทำให้เสี่ยง deadlock (ดู ARCHITECTURE.md §7)

### 7.6 ready check

```go
func (c *KafkaConsumer) checkReadyToAccept() error {
    defer func() { c.isReady = true }()
    for {
        select {
        case <-c.readyCH:
            return nil
        default:
            time.Sleep(1 * time.Second)
            isReady, err := c.readyCheck()
            ...
            if isReady { return nil }
        }
    }
}

func (c *KafkaConsumer) readyCheck() (bool, error) {
    assignment, err := c.Consumer.Assignment()   // partition ที่ถูก assign
    return len(assignment) > 0, nil               // มี partition = พร้อม
}
```
- วนเช็คทุก 1 วิ ว่า consumer ได้รับ partition มาดูแลแล้วหรือยัง (`Assignment()` คืน list partition ที่ถือ)
- ได้ partition (len > 0) = พร้อมรับงาน

### 7.7 initializeKafkaTopic / waitForTopicReady

- `initializeKafkaTopic` — ใช้ `AdminClient` สร้าง topic (`NumPartitions: 1`) ถ้ายังไม่มี (ถ้ามีแล้วก็ข้าม), แล้วรอจน topic พร้อม
- `waitForTopicReady` — วนถาม metadata จน leader ของทุก partition พร้อม (`partition.Leader != -1`) ก่อนปล่อยให้อ่าน

---

## Step 8 — `cmd/main.go`

ประกอบทุกอย่างเข้าด้วยกัน + รัน loop หลัก

```go
type Server struct {
    producer  *producer.KafkaProducer
    consumer  *consumer.KafkaConsumer
    msgCH     chan *shared.Message
    eventRepo *repo.EventRepo
}
```
- `Server` รวมทุก component ไว้ในที่เดียว (producer, consumer, channel, repo)

```go
func NewServer(eventRepo *repo.EventRepo) *Server {
    msgCH := make(chan *shared.Message, 64)
    c, err := consumer.NewKafkaConsumer(msgCH)
    if err != nil {
        panic(err)
    }
    return &Server{
        producer:  producer.NewKafkaProducer(""),
        consumer:  c,
        msgCH:     msgCH,
        eventRepo: eventRepo,
    }
}
```
- `make(chan *shared.Message, 64)` — สร้าง **buffered channel** ขนาด 64 (อุ้ม message ได้ 64 ตัวก่อนบล็อก — เผื่อ handler ช้ากว่า consumer)
- สร้าง consumer (ส่ง msgCH เข้าไปให้มันยิงเข้า) + producer (`""` = ใช้ topic default)
- consumer สร้างไม่สำเร็จก็ panic

```go
func (s *Server) produceMsg() {
    ticket := time.NewTicker(time.Second)
    defer ticket.Stop()
    for range ticket.C {
        event := repo.NewEvent()
        payload, err := json.Marshal(event)
        if err != nil {
            fmt.Printf("error marshalling event = %v\n", err)
            continue
        }
        s.producer.Producer(string(payload))
    }
}
```
- `time.NewTicker(time.Second)` — ticker เต้นทุก 1 วิ, `defer ticket.Stop()` — หยุด ticker ตอนจบ
- `for range ticket.C` — วนทุกครั้งที่ ticker เต้น (ไม่สนค่าเวลา)
- สร้าง Event → `json.Marshal` เป็น JSON → ยิงผ่าน producer (ส่ง JSON ตรงกับที่ consumer จะ unmarshal)

```go
func (s *Server) handleMsg(msg *shared.Message) {
    ctx := context.Background()
    s.saveToDB(ctx, msg)
}
```
- handler ของแต่ละ message — สร้าง context เปล่าแล้วส่งต่อไป saveToDB

```go
func (s *Server) saveToDB(ctx context.Context, msg *shared.Message) {
    repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
        defer s.consumer.MarkAsComplete(msg.Metadata)   // จบเมื่อไหร่ก็ mark เสร็จ

        event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)
        if event != nil {                                 // เคยมีแล้ว → ข้าม
            eMsg := fmt.Sprintf("offset = %d, eventID %s already existing -> skipping\n", msg.Metadata.Offset, msg.Event.EventId)
            return "", errors.New(eMsg)
        }
        id, err := s.eventRepo.Insert(ctx, tx, msg.Event) // ยังไม่มี → insert
        if err != nil {
            return "", err
        }
        return id, nil
    })
}
```
- เรียก `TxClosure` แล้วส่ง **closure** (logic ที่จะทำใน transaction) เข้าไป
- `defer s.consumer.MarkAsComplete(msg.Metadata)` — ไม่ว่าผลเป็นไง (insert สำเร็จ หรือ skip เพราะซ้ำ) ก็ mark offset นี้ว่า "เสร็จ" → commit loop จะ commit ผ่านได้
- `Get` ก่อน → ถ้าเจอ (`!= nil`) คืน error "skipping" (idempotent — กันทำซ้ำ)
- ถ้าไม่เจอ → `Insert`
- ค่า return ของ closure ตัดสินว่า TxClosure จะ commit (nil error) หรือ rollback (มี error)

```go
func main() {
    db, err := repo.NewDBConn()
    if err != nil {
        panic(err)
    }
    er := repo.NewEventRepo(db)
    s := NewServer(er)

    go s.produceMsg()                  // เริ่มยิง event (goroutine)
    for msg := range s.msgCH {         // main: รับ message จาก channel ไม่หยุด
        go s.handleMsg(msg)            // แตก goroutine process แต่ละตัว (async)
    }
}
```
- ต่อ DB → สร้าง repo → สร้าง Server (ซึ่งสร้าง consumer + producer ข้างใน)
- `go s.produceMsg()` — ยิง event แยก goroutine
- `for msg := range s.msgCH` — **main goroutine** วนรับ message จาก channel (บล็อกรอจนกว่ามี message) — channel ไม่ปิด เลยรันตลอด
- `go s.handleMsg(msg)` — แต่ละ message แตก goroutine ใหม่ → process **ขนานกัน** (นี่คือที่มาของการเสร็จไม่เรียงลำดับ → ต้องมี sequential commit)

---

## สรุปภาพรวมการประกอบ

```
main()
 ├─ NewDBConn()                    Step 3
 ├─ NewEventRepo(db)               Step 4
 └─ NewServer(er)
      ├─ make(msgCH, 64)
      ├─ NewKafkaConsumer(msgCH)   Step 7  → เปิด 3 goroutine (read/commit/ready)
      └─ NewKafkaProducer("")      Step 6  → เปิด 1 goroutine (delivery report)

 ├─ go produceMsg()   → ทุก 1 วิ: NewEvent → Marshal → Produce
 └─ for range msgCH   → go handleMsg → saveToDB (TxClosure: Get→Insert→MarkAsComplete)
```

goroutine ที่วิ่งพร้อมกันตอนรัน:
1. `produceMsg` — ยิง event
2. producer delivery-report listener
3. consumer `readMsgLoop` — อ่าน
4. consumer `commitOffsetLoop` — commit ทุก 15 วิ
5. consumer `checkReadyToAccept` — เช็คพร้อม
6. main loop + `handleMsg` แต่ละตัว — process

ทั้งหมดเชื่อมกันด้วย `msgCH` (channel) และ `msgsStateMap` (shared state + mutex) — นี่คือหัวใจที่ทำให้ทุกเส้นทำงานขนานกันได้โดยไม่ชนกัน

---

> อ่านคู่กับ `ARCHITECTURE.md` (ภาพรวม + กลไกเชิงลึก + rebuild guide + known issues) จะเห็นภาพครบทั้งระดับบรรทัดและระดับระบบ



