# Kafka Rebalance — อธิบาย code (per-partition state + commit before revoke)

เอกสารนี้อธิบาย rebalance implementation ที่เพิ่มเข้ามา — กลไกที่ทำให้ consumer **scale หลาย pod** ได้อย่างปลอดภัย (เพิ่ม/ลด consumer แล้ว message ไม่หาย ไม่ซ้ำเกินจำเป็น)

> Rebalance = เมื่อ consumer เพิ่ม/หายในกลุ่ม Kafka จะ **แจก partition ใหม่** ให้สมาชิก ตอน partition ถูกยึดคืน ต้อง commit offset ที่ค้างก่อนปล่อย ไม่งั้น consumer ตัวใหม่อ่านซ้ำเยอะ

---

## สิ่งที่เปลี่ยนจากเวอร์ชันเดิม (สำคัญ)

| เดิม (single partition) | ใหม่ (rebalance-ready) |
|---|---|
| `msgsStateMap map[Offset]bool` | `msgsStateMap map[int32]*PartitionState` — แยก state **ต่อ partition** |
| state เป็น `bool` (เสร็จ/ไม่เสร็จ) | `MsgState` **enum**: Pending / Success / Error |
| commit loop ตัวเดียว | commit loop **ต่อ partition** (1 goroutine/partition) |
| ไม่มี rebalance handler | `rebalanceCB` → assign/revoke callback |
| `maxReceived` / `lastCommited` ธรรมดา | เป็น `atomic.Int32` (thread-safe) |
| hardcode `Partition: 0` | ทำงานทุก partition ที่ถูก assign |

นี่คือการ implement ตาม `ARCHITECTURE.md §8.4` จริง — per-partition state คือหัวใจ

---

## 1. MsgState — enum 3 สถานะ

```go
type MsgState = int32
const (
    MsgState_Pending MsgState = iota   // 0 — เพิ่งอ่าน ยังไม่ process
    MsgState_Success MsgState = iota   // 1 — process สำเร็จ
    MsgState_Error   MsgState = iota   // 2 — process พลาด
)
```
- `iota` — ตัวนับอัตโนมัติของ Go เริ่ม 0 เพิ่มทีละ 1 (Pending=0, Success=1, Error=2)
- `type MsgState = int32` — type alias (`=` คือ alias ไม่ใช่ type ใหม่)
- ต่างจากเดิมที่เป็น `bool` — ตอนนี้แยก Error ออกจาก Success ได้ (ตรงกับ flowchart ใน MESSAGE_LIFECYCLE.md)

## 2. PartitionState — state แยกต่อ partition (`parition-state.go`)

```go
type PartitionState struct {
    ID    int32              // partition number
    Topic *string
    mu    *sync.RWMutex
    state map[kafka.Offset]MsgState   // offset → สถานะ ของ partition นี้

    maxReceived  *atomic.Int32        // offset สูงสุดที่อ่านมา
    lastCommited *atomic.Int32        // commit ถึงไหนแล้ว

    ctx    context.Context            // ใช้สั่งหยุด commit loop
    cancel context.CancelFunc
    exitCH chan struct{}              // สัญญาณว่า loop หยุดแล้วจริง
}
```
- **ทำไมแยกต่อ partition** — offset ของแต่ละ partition นับแยกกัน (P0 offset 3 ≠ P1 offset 3) ถ้าใช้ map รวมจะชนกัน
- **`atomic.Int32`** — `maxReceived`/`lastCommited` ถูกอ่าน/เขียนจากหลาย goroutine (read loop เขียน, commit loop อ่าน) ใช้ atomic แทน lock ตรงๆ ได้ เร็วกว่า
- **`ctx` + `cancel` + `exitCH`** — กลไกหยุด commit loop ของ partition นี้ตอนถูก revoke (cancel สั่งหยุด → loop เห็น ctx.Done() → close exitCH ยืนยันว่าหยุดแล้ว)

```go
func NewPartitionState(tp *kafka.TopicPartition) *PartitionState {
    ctx, cancel := context.WithCancel(context.Background())
    initialLastCommited := tp.Offset - 1
    if tp.Offset == kafka.OffsetBeginning || tp.Offset < 0 {
        initialLastCommited = -1
    }
    ...
}
```
- เริ่ม `lastCommited = tp.Offset - 1` (committed offset คือ "ตัวถัดไปจะอ่าน" → ตัวล่าสุดที่ทำคือ offset-1) ← นี่ตอบเรื่อง offset-1 ที่ผมตั้งคำถามใน ARCHITECTURE §7 ข้อ 2 — มันตั้งใจ
- ถ้าเริ่มจากต้น (OffsetBeginning หรือ < 0) ตั้ง -1 (ยังไม่เคย commit อะไร)

## 3. Config สำหรับ rebalance (`consumer.go`)

```go
kafka.NewConsumer(&kafka.ConfigMap{
    "enable.auto.commit":              false,
    "auto.offset.reset":               "earliest",
    "go.application.rebalance.enable": true,                          // ← เปิดให้เราคุม rebalance เอง
    "partition.assignment.strategy":   string(cfg.ParititionAssignStrategy),  // cooperative-sticky
})
```
- `go.application.rebalance.enable: true` — บอก library ว่า "ฉันจะจัดการ rebalance เอง" (เรียก callback ที่เราให้) ไม่ปล่อยให้ library assign อัตโนมัติ
- `partition.assignment.strategy` — วิธีแจก partition (ดู §6)

```go
c.SubscribeTopics([]string{consumer.topic}, consumer.rebalanceCB)
```
- arg ที่ 2 คือ **rebalance callback** — จะถูกเรียกทุกครั้งที่มี rebalance

## 4. rebalanceCB — ตัวกระจายงาน

```go
func (c *KafkaConsumer) rebalanceCB(_ *kafka.Consumer, event kafka.Event) error {
    switch ev := event.(type) {
    case kafka.AssignedPartitions:   // ได้ partition ใหม่
        return c.assignPrntCB(&ev)
    case kafka.RevokedPartitions:    // ถูกยึด partition คืน
        return c.revokePrtnCB(&ev)
    }
    return nil
}
```
- type switch แยก 2 เหตุการณ์: ได้ partition (Assigned) / เสีย partition (Revoked)

## 5. assignPrntCB — ตอนได้ partition ใหม่

```go
committed, err := c.consumer.Committed(ev.Partitions, int(time.Second)*5)  // ถามว่าเคย commit ถึงไหน
for _, tp := range committed {
    startOffset := tp.Offset
    if startOffset < 0 { startOffset = kafka.OffsetBeginning }   // ไม่เคย commit → เริ่มต้น

    prtnState := NewPartitionState(&tpCopy)                       // สร้าง state ใหม่ของ partition นี้
    oldPS, exists := c.msgsStateMap[tp.Partition]
    if exists {
        oldPS.cancel(); <-oldPS.exitCH; oldPS = nil               // ถ้ามีของเก่า → หยุดก่อน
    }
    c.msgsStateMap[tp.Partition] = prtnState
    go prtnState.commitOffsetLoop(c.commitDur, c)                 // เปิด commit loop ของ partition นี้
}

if cooperative-sticky { c.consumer.IncrementalAssign(...) }       // เพิ่มทีละ partition
else                  { c.consumer.Assign(...) }                  // assign ทั้งชุด
```
ขั้นตอน: ถามตำแหน่ง commit เดิม → สร้าง `PartitionState` ต่อ partition → เปิด commit loop (goroutine) ของแต่ละตัว → แล้วค่อยรับ partition จริง

## 6. revokePrtnCB — ตอนถูกยึด partition (หัวใจของความปลอดภัย)

```go
for _, tp := range ev.Partitions {
    partitionState := c.msgsStateMap[tp.Partition]
    partitionState.cancel()              // สั่งหยุด commit loop ของ partition นี้
    <-partitionState.exitCH              // รอจนมันหยุดจริง

    latestToCommit, _ := partitionState.findLatestToCommit()   // หา offset ที่ commit ได้
    if latestToCommit >= 0 {
        toCommit = append(toCommit, ...)                        // เก็บไว้ commit
    }
    delete(c.msgsStateMap, tp.Partition)                        // ลบ state
}

if len(toCommit) > 0 {
    c.consumer.CommitOffsets(toCommit)   // ✅ COMMIT ก่อนปล่อย partition!
}

if cooperative-sticky { c.consumer.IncrementalUnassign(...) }
else                  { c.consumer.Unassign() }
```
**นี่คือจุดสำคัญที่สุดของ rebalance** — ก่อนปล่อย partition ให้ pod อื่น ต้อง:
1. หยุด commit loop (cancel + รอ exitCH)
2. commit งานที่ทำเสร็จแล้วทั้งหมด (`CommitOffsets`)
3. ค่อยปล่อย (Unassign)

ถ้าไม่ commit ก่อนปล่อย → pod ใหม่ที่รับ partition ไปจะอ่านซ้ำตั้งแต่ committed offset เก่า = ทำงานซ้ำเยอะโดยไม่จำเป็น

## 7. commitOffsetLoop + findLatestToCommit (ต่อ partition)

ตอนนี้ commit loop อยู่ใน `PartitionState` แต่ละตัว (ไม่ใช่ของ consumer รวม) — แต่ละ partition commit อิสระ

`findLatestToCommit` = sequential scan เวอร์ชัน enum:
```go
for offset := lastCommited; offset <= latestToCommit; offset++ {
    msgState, exists := ps.state[offset]
    if !exists { continue }
    if msgState != MsgState_Pending {        // Success หรือ Error → ผ่านได้
        delete(ps.state, offset)
        if len(ps.state) == 0 { latestToCommit = offset + 1; break }
        continue
    }
    latestToCommit = offset                   // เจอ Pending → หยุด (กำแพง)
    break
}
```
- ตรรกะเดิม (เจอ Pending หยุด) แต่ตอนนี้ **Error ก็ผ่านได้** (เฉพาะ Pending ที่บล็อก) — Error ถือว่า "จัดการแล้ว" ไม่ขวาง commit

## 8. cooperative-sticky vs eager (`kafka-config.go`)

```go
const (
    CooperativeStickyStrategy = "cooperative-sticky"   // ← ใช้ตัวนี้
    RoundRobin                = "roundrobin"
)
```
- **eager (roundrobin)** — ตอน rebalance ทุก consumer **ปล่อย partition ทั้งหมดก่อน** แล้วแจกใหม่ = "stop-the-world" ทุกตัวหยุดทำงานชั่วคราว
- **cooperative-sticky** — แจกแบบ **incremental** ปล่อยเฉพาะ partition ที่ต้องย้ายจริง ตัวที่ไม่ต้องย้ายทำงานต่อได้เลย = downtime น้อยกว่ามาก เหมาะกับ production (เลยใช้ `IncrementalAssign`/`IncrementalUnassign`)

## 9. repo-err.go — ตรวจ duplicate key

```go
func IsDuplicateKeyErr(err error) bool {
    var pgErr *pq.Error
    if errors.As(err, &pgErr) {
        return pgErr.Code == pq.ErrorCode("23505")   // PG code: unique_violation
    }
    return false
}
```
- ตรวจว่า error จาก insert เป็น "duplicate key" (PostgreSQL error code `23505`) ไหม
- ใช้ทำ idempotent แบบพึ่ง DB constraint แทนการ `Get` ก่อน (insert ตรงๆ ถ้าซ้ำก็จับ error นี้) — เร็วกว่าเพราะไม่ต้อง query 2 รอบ

---

## ⚠️ สิ่งที่ต้องแก้ — `main.go` ยังไม่ sync กับ consumer ใหม่

โค้ด consumer เปลี่ยน signature แต่ `main.go` ยังเป็นของเก่า จะ **compile ไม่ผ่าน** จนกว่าจะแก้:

1. `NewKafkaConsumer(msgCH)` ตอนนี้คืน **`*KafkaConsumer` ตัวเดียว** (ไม่มี error) แต่ main เขียน `c, err := ...` → ต้องแก้เป็น `c := consumer.NewKafkaConsumer(msgCH)`
2. consumer ใหม่ **ไม่มี `MarkAsComplete`** แล้ว → เปลี่ยนเป็น `UpdateState(tp, MsgState_Success)` หรือ `MsgState_Error` ตามผล
3. ต้องเรียก `RunConsumer()` (ตัวเริ่ม `consumeLoop` + `checkReadyToAccept`) — ตอนนี้ main ไม่ได้เรียก
4. `saveToDB` ควรส่งผลจริงเข้า `UpdateState` (Success/Error) แทนที่จะ mark complete เฉยๆ ทุกกรณี — เพื่อให้ enum Error ทำงานตามที่ออกแบบ

> นี่ไม่ใช่บั๊กของ rebalance code — แค่ main.go ยังตามไม่ทัน เป็นขั้นตอนปกติของการ refactor อยากให้ผมช่วย sync main.go ให้เข้ากับ consumer ใหม่ก็บอกได้

---

> อ่านคู่กับ `ARCHITECTURE.md §8` (scaling), `MESSAGE_LIFECYCLE.md` (flow + enum state) และ `LEARNING_ROADMAP.md` (ก้าวต่อไป)
