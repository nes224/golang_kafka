# Message Lifecycle — ชีวิตของ message 1 ตัว (รับ → process → commit)

เอกสารนี้ไล่ flow การทำงานของ consumer ตั้งแต่ message มาถึง จนถูก commit — อธิบายทุก step + decision point และ map กลับเข้าโค้ดจริง

> สรุป 1 บรรทัด: message ถูกอ่าน → จด state = Pending → ส่งเข้า channel → process ลง DB → อัปเดต state → commit loop ยิงเป็นรอบ scan หา offset ต่อเนื่องที่เสร็จแล้ว → commit

---

## Flowchart รวม

```
                  ┌─────────────────┐
                  │ Message Arrives │
                  └────────┬────────┘
                           ▼
                  ┌─────────────────────┐
                  │ ReadMessage จาก Kafka│         ← readMsgLoop
                  └────────┬────────────┘
                           ▼
                  ╱───────────────────╲
                 ╱ Partition State      ╲   No   ┌──────────────────────┐
                 ╲ Exists?              ╱───────▶│ Error: Missing Partition│
                  ╲───────────────────╱         └──────────────────────┘
                           │ Yes
                           ▼
                  ┌─────────────────┐
                  │ appendMsgState()│
                  │ state[offset]=Pending
                  │ maxReceived.Store(offset)
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │ Send to msgCH   │            ← ส่งเข้า channel
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │ Process in DB   │            ← handleMsg → saveToDB (TxClosure)
                  └────────┬────────┘
                           ▼
                  ╱───────────────────╲
                 ╱   DB Result?        ╲
                  ╲───┬────────┬───────╱
         Success/     │        │  Error
         Duplicate    ▼        ▼
        ┌──────────────────┐ ┌──────────────────┐
        │ UpdateState(Succ)│ │ UpdateState(Error)│
        │ state[off]=Success│ │ state[off]=Error │
        └────────┬─────────┘ └────────┬─────────┘
                 └─────────┬───────────┘
                           ▼
                  ┌─────────────────────┐
                  │ Wait for Commit Loop│        ← commitOffsetLoop (goroutine แยก)
                  └────────┬────────────┘
                           ▼
                  ┌─────────────────┐
                  │ 10s Timer Fires │            ← ticker (โค้ดจริง = 15s)
                  └────────┬────────┘
                           ▼
                  ┌─────────────────────────┐
                  │ findLatestToCommit()    │
                  │ Scan lastCommitted→maxReceived
                  └────────┬────────────────┘
                           ▼
              ┌──────────╱───────────────╲◀────────────┐
              │         ╱  Message State?  ╲            │ Yes (มี offset ถัดไป)
              │         ╲─┬──────────┬─────╱            │
              │  Pending  │          │ Success/Error    │
              │           │          ▼                  │
              │           │  ┌────────────────────┐     │
              │           │  │ delete(state,offset)│     │
              │           │  │ Continue scanning  │─────┤
              │           │  └────────────────────┘     │
              │           │          ▼                  │
              │           │   ╱───────────────╲ Yes     │
              │           │  ╱ More messages    ╲────────┘
              │           │  ╲ to scan?         ╱
              │           │   ╲───────────────╱
              │           │          │ No
              │           ▼          ▼
              │  ┌───────────────────────────┐
              └─▶│ Stop at this offset       │   ← เจอ Pending = หยุด
                 │ latestToCommit = offset   │
                 └────────────┬──────────────┘
                              ▼
                 ┌───────────────────────────┐
                 │ CommitOffsets(latestToCommit)│  ← commit จริงเข้า Kafka
                 └───────────────────────────┘
```

---

## ช่วง A — รับ message เข้า (readMsgLoop)

| Step | ทำอะไร | โค้ด |
|---|---|---|
| **Message Arrives** | มี message ใหม่ใน partition | — |
| **ReadMessage** | อ่าน message จาก Kafka (รอ 1 วิ) | `c.Consumer.ReadMessage(time.Second)` |
| **Partition State Exists?** | เช็คว่ามี state ของ partition นี้แล้วยัง | *(ยังไม่มีในโค้ด — ดู §ความต่าง)* |
| **appendMsgState()** | จด `state[offset]=Pending` + อัปเดต maxReceived | `c.appendMsgState(&msg.TopicPartition)` |
| **Send to msgCH** | โยน message เข้า channel ให้ handler | `c.msgCH <- payload` |

**ทำไม Pending ก่อน** — พอเพิ่งอ่านมา ยังไม่ได้ process จึงตั้ง Pending ไว้ก่อน เพื่อบอก commit loop ว่า "ตัวนี้ยังค้างอยู่ อย่าเพิ่ง commit ข้าม" และ `maxReceived` บันทึกว่าอ่านไปไกลสุดถึง offset ไหน (ขอบบนของการ scan)

---

## ช่วง B — ประมวลผล (handleMsg → saveToDB)

| Step | ทำอะไร | โค้ด |
|---|---|---|
| **Process in DB** | เปิด transaction → Get เช็คซ้ำ → Insert | `repo.TxClosure(... Get → Insert ...)` |
| **DB Result?** | แยกผล 3 ทาง | return value ของ closure |
| **Success / Duplicate** | สำเร็จ หรือ มีอยู่แล้ว (idempotent) | `Insert` ok / `Get != nil` |
| **Error** | insert พลาดจริง | `Insert` คืน error |
| **UpdateState** | ตั้ง state ตามผล | `MarkAsComplete(msg.Metadata)` |

**Duplicate ถือเป็นจบงาน** — ถ้า `Get` เจอ event อยู่แล้ว = เคย process ไปแล้ว (idempotent) ก็ถือว่า "เสร็จ" ไม่ต้องทำซ้ำ จึงไป UpdateState(Success) เหมือนกัน เพื่อให้ commit ผ่าน offset นี้ได้

---

## ช่วง C — commit loop (commitOffsetLoop)

| Step | ทำอะไร | โค้ด |
|---|---|---|
| **Timer Fires** | ตื่นทุก N วิ | `time.NewTicker(c.commitDur)` |
| **Scan lastCommitted→maxReceived** | ไล่ดู state ทีละ offset | `for offset := c.lastCommited; offset < c.maxReceived.Offset; offset++` |
| **Message State?** | เช็ค state ของ offset นั้น | `completed, exists := c.msgsStateMap[offset]` |
| **Pending → Stop** | เจอตัวยังไม่เสร็จ = หยุด | `latestToCommit.Offset = offset; break` |
| **Success/Error → delete + ต่อ** | เสร็จแล้ว ลบทิ้ง scan ต่อ | `delete(c.msgsStateMap, offset)` |
| **CommitOffsets** | commit ถึงจุดที่หยุด | `c.Consumer.CommitOffsets([]kafka.TopicPartition{latestToCommit})` |

**หัวใจ — เจอ Pending คือกำแพง** ไล่ scan จาก `lastCommitted` ขึ้นไป ตราบใดที่เจอ Success/Error ก็ลบออกจาก map แล้วเดินต่อ พอ**เจอ Pending ตัวแรก** ก็หยุดทันที (`break`) commit ได้แค่ถึงก่อนหน้านั้น — นี่คือ **sequential commit** (ห้ามข้ามรูโหว่) ที่ทำให้ message ไม่หายตอน crash

**ทำไม Error ก็ delete + ผ่านได้** — เพราะ Error ถือว่า "จัดการแล้ว" (จะ retry แยก หรือ log ไว้) ไม่บล็อกการ commit ถ้าปล่อยให้ Error ค้างเป็น Pending ตลอด commit จะติดที่ offset นั้นถาวร

---

## Mapping flowchart ↔ ฟังก์ชันในโค้ด

```
readMsgLoop()        → Message Arrives, ReadMessage, appendMsgState, Send to msgCH
handleMsg/saveToDB() → Process in DB, DB Result, UpdateState (ผ่าน MarkAsComplete)
commitOffsetLoop()   → Timer, Scan, Message State, delete, Stop, CommitOffsets
```

---

## ความต่างจากโค้ดปัจจุบัน (อ่านตามจริง — ไม่ได้แต่ง)

flowchart นี้เป็นเวอร์ชัน **อุดมคติ/ปรับปรุงแล้ว** มีบางจุดที่โค้ดตอนนี้ยังไม่ตรงเป๊ะ:

1. **"Partition State Exists?" + Error: Missing Partition** — โค้ดปัจจุบัน**ยังไม่มี**เช็คนี้ เพราะเป็น single partition (`Partition: 0`) `appendMsgState` ไม่ได้ตรวจ partition state จะมีก็ต่อเมื่อทำ per-partition state ตาม `ARCHITECTURE.md §8.4`

2. **state เป็น 3 ค่า (Pending/Success/Error)** — flowchart แยก Success กับ Error แต่โค้ดจริง `msgsStateMap` เป็น `map[Offset]bool` (แค่ true/false) และ `MarkAsComplete` ตั้ง `true` **เสมอ** ไม่ว่าผล insert จะสำเร็จ duplicate หรือ error (เพราะ `defer MarkAsComplete` ไม่สน return value) → ถ้าอยากแยก Error จริงต้องเปลี่ยน type เป็น enum + ส่งผลเข้า MarkAsComplete

3. **Timer 10s** — รูปเขียน 10 วิ แต่โค้ดจริง `commitDur = 15 * time.Second`

4. **commit loop เสี่ยง deadlock** — ตามที่ระบุใน `ARCHITECTURE.md §7` ข้อ 1 (`continue` ขณะถือ lock) — flowchart ไม่ได้แสดงจุดนี้

ถ้าจะทำให้โค้ดตรงกับ flowchart นี้เป๊ะ = งานเดียวกับ "Scaling to production" (§8) + เปลี่ยน state เป็น enum — เก็บเป็นแผนรอบหน้าได้

---

> อ่านคู่กับ `ARCHITECTURE.md` (ภาพรวม + scaling) และ `CODE_WALKTHROUGH.md` (โค้ดทีละบรรทัด) จะเห็นครบทั้ง flow, โครงสร้าง, และรายละเอียดบรรทัด
