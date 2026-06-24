# Learning Log — ไล่โค้ด golang_kafka + เทียบ golang_pubsub

> บันทึก session ไล่อ่านโค้ดทีละ function · อัปเดต: 2026-06-16
> ใช้คู่กับ `CODE_WALKTHROUGH.md` (ไล่ตาม build order) — ไฟล์นี้คือ "เราคุยถึงไหนแล้ว + key takeaway"

---

## สถานะ: คุยถึงไหนแล้ว

```
ฝั่ง PRODUCER  ✅ จบครบ
ฝั่ง CONSUMER  🔄 กำลังไล่ — ถึง consumeLoop() แล้ว
ถัดไป          → concurrent goroutine (เมฆจะไปอ่านเรื่องนี้ต่อ)
```

---

## ✅ ฝั่ง Producer — จบครบแล้ว

ไล่ตามลำดับ: struct → Produce → produceMsg → NewKafkaProducer

| ชิ้น | สรุปสั้น |
|---|---|
| `KafkaProducer` struct | เก็บ 2 field: `producer *kafka.Producer` (client/ปืน) + `topic string` · `*` = pointer แชร์ตัวเดียว |
| `Produce(msg)` | **ตัวยิงจริง** · async/fire-and-forget · ไม่บล็อก ไม่คืน error · `PartitionAny` = broker เลือก partition เอง · arg ที่ 2 = nil → ใช้ `p.Events()` กลาง |
| `produceMsg()` (main) | **คนสั่งยิงทุก 1 วิ** · NewEvent → Marshal → Produce |
| `NewKafkaProducer()` | **คนประกอบปืน** · สร้าง client + ตั้ง goroutine อ่านใบเสร็จ (ครั้งเดียว) + คืน struct |
| goroutine `p.Events()` | **คนอ่านใบเสร็จ** (delivery report) · ไม่ได้สร้าง partition/offset · แค่อ่านผลว่า message ไปลง PRTN ไหน OFFSET เท่าไหร่ (broker เป็นคนให้ offset) |

**Key takeaways ฝั่ง producer:**
- offset broker เป็นคนกำหนด · เดินแยกอิสระต่อ partition (0,1,2,... ต่อเลน) ไม่มี global offset
- Kafka producer = async (ผลมาทีหลังทาง Events) · Pub/Sub = sync (`res.Get()` รอผลเลย)
- `Produce` ไม่คืน error → ถ้าไม่อ่าน delivery report จะไม่รู้ว่าส่ง fail

---

## 🔄 ฝั่ง Consumer — กำลังไล่

### คุยไปแล้ว

| ชิ้น | สรุปสั้น |
|---|---|
| `KafkaConsumer` struct | ~12 field · แบ่ง 4 กลุ่ม: core / readiness / channel (`MsgCH`,`exitCH`) / state+commit (`msgsStateMap`,`mu`,`commitDur`) |
| `ID` (random string) | ป้ายชื่อ debug ตอนรันหลาย instance · **ไม่ใช่** identity ที่ Kafka ใช้ (นั่นคือ `group.id` + member id) |
| `NewKafkaConsumer()` config | `enable.auto.commit:false` (commit เอง = หัวใจ) · `auto.offset.reset:earliest` (อ่านจากตัวแรก) · `rebalance.enable:true` (ขอ callback เอง) · `cooperative-sticky` (ย้ายเฉพาะที่จำเป็น) |
| ประกอบ struct + `initializeKafkaTopic()` | ยัด field · `MsgCH` รับมาจาก Server (ท่อเดียว 2 คนถือ) · สร้าง topic 4 partition ที่นี่ (ก่อน subscribe) |
| `group.id` / consumer group | 1 group แบ่ง partition กันในทีม (ไม่ซ้ำ) · scale ได้สูงสุด = จำนวน partition · หลาย group = ต่างได้ message ครบ (offset แยกต่อ group) |
| `consumeLoop()` | **หัวใจขาเข้า** · วน `ReadMessage(1s)` → กรอง timeout/error/nil → `appendMsgState` (mark Pending) → โยนเข้า `MsgCH` ผ่าน `select`+timeout 5s (ตันเกิน 5s = ทิ้ง+Error) |

**Key takeaways ฝั่ง consumer (ถึงตอนนี้):**
- 3 config (`auto.commit:false` + `rebalance.enable` + `assignment.strategy`) คือเหตุผลที่ consumer บวมเป็น 420 บรรทัด — เพราะ "ขอทำ commit + rebalance เอง"
- topic / consumer group / consumer = 3 คำคนละสิ่ง (กล่อง / ทีม / สมาชิก)
- partition = เพดาน scale + ตัวกำหนดความขนาน (1 partition ต่อ 1 consumer ในกลุ่ม ณ เวลานึง)

### ⏸️ ค้างตรงนี้ — ยังไม่ได้คุย

```
appendMsgState()        ← บันทึก offset = Pending ลง PartitionState   [ตัวถัดไป]
UpdateState()           ← handler รายงานผลกลับ (Success/Error)
PartitionState struct   ← state ต่อ 1 partition (parition-state.go)
commitOffsetLoop()      ← วน commit ทุก 10 วิ
findLatestToCommit()    ← เลือก offset ที่ commit ได้ (sequential — ห้ามข้ามรู)
rebalanceCB/assign/revoke ← ตอน partition ย้าย   [ยากสุด เก็บท้าย]
checkReadyToAccept/readyCheck ← เช็คพร้อมรับงาน
RunConsumer()           ← ปุ่ม start ยิง 2 goroutine
```

---

## ⏭️ ถัดไป: Concurrent Goroutine (เมฆจะไปอ่านต่อ)

เรื่องนี้สำคัญมากกับฝั่ง consumer เพราะโปรเจกต์นี้ใช้ concurrency เยอะ ตรงที่ควรโยงเวลาอ่าน:

**1. goroutine ที่รันขนานในระบบนี้**
```
go s.consumer.RunConsumer()  → ข้างในยิงอีก 2: consumeLoop + checkReadyToAccept
go s.produceMsg()            → ยิง event ทุกวิ
go func(){ p.Events() }()    → อ่านใบเสร็จ producer
go s.handleMsg(msg)          → ★ fan-out: 1 message = 1 goroutine (process ขนาน)
go prtnState.commitOffsetLoop() → commit แยกต่อ partition
```

**2. ของที่ใช้ "คุย/กันชน" ระหว่าง goroutine**
- **channel** — `MsgCH` (consumeLoop → handleMsg), `ReadyCH`/`exitCH` (สัญญาณ ปิด/พร้อม ด้วย `close()`)
- **`select` + `time.After`** — กันบล็อกถาวร (เห็นใน consumeLoop ตอนโยน MsgCH)
- **mutex** — `c.mu *sync.RWMutex` (กัน `msgsStateMap`), `ps.mu` (กัน `state map` ใน partition)
- **atomic** — `maxReceived`, `lastCommited` (`atomic.Int32`) ใน PartitionState

**3. ทำไม consumer ต้องใช้ concurrency หนัก**
- อ่าน (consumeLoop) / process (handleMsg) / commit (commitOffsetLoop) **แยก goroutine** กัน → ต้องมี state ที่หลายตัวแตะร่วม → เลยต้องมี mutex/atomic/channel
- `go handleMsg` ทำให้ process **ขนานหลาย message พร้อมกัน** → offset เสร็จไม่เรียงกัน → นี่คือที่มาของ "sequential commit" (commitOffsetLoop + findLatestToCommit)

> 💡 จุดเชื่อม: พอเข้าใจ concurrent goroutine แล้วกลับมาอ่าน `commitOffsetLoop` + `findLatestToCommit` จะเก็ตทันทีว่าทำไมต้อง lock + ทำไม commit ต้องเรียงไม่ข้ามรู

---

## เทียบ golang_pubsub (สรุปที่ได้จาก session นี้)

| Kafka (กำลังเรียน) | golang_pubsub (ที่สร้างไว้) |
|---|---|
| producer async + goroutine อ่าน Events | `res.Get(ctx)` sync บรรทัดเดียว |
| `consumeLoop` วน ReadMessage เอง | `sub.Receive()` push เข้า callback ให้ |
| `MsgCH` + `go handleMsg` fan-out เอง | Receive เรียก handler ขนานให้เอง (`MaxOutstandingMessages`) |
| `appendMsgState` + commit loop + sequential | ❌ ไม่มี — `Ack()`/`Nack()` ทีละ message |
| rebalance assign/revoke เขียนเอง | ❌ ไม่มี — server แบ่งให้ |
| offset (track เอง) | ❌ ไม่มี offset |
| ต้องกัน race ด้วย mutex/atomic เยอะ | concurrency ส่วนใหญ่ Pub/Sub จัดให้ |

**บทสรุปใหญ่:** concurrency machinery เกือบทั้งหมดของ Kafka consumer (channel/mutex/atomic/commit loop) มีไว้เพื่อ "จัดการ offset เอง + process ขนาน" — Pub/Sub ยกงานนี้ไปทำฝั่ง server เลยเหลือโค้ดนิดเดียว

---

## กลับมาต่อจากตรงไหน

เปิดไฟล์นี้ → ไปที่ section "⏸️ ค้างตรงนี้" → ตัวถัดไปคือ `appendMsgState()` (คู่กับ `UpdateState()`)
หรือถ้าเพิ่งอ่าน concurrency จบ → กระโดดไป `commitOffsetLoop` + `findLatestToCommit` ได้เลย (จะเข้าใจง่ายขึ้นเยอะ)
