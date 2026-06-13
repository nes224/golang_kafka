# Kafka — คำศัพท์ที่ใช้ (สรุปจากที่คุยกัน)

> รวมคำศัพท์ทางเทคนิคทั้งหมดที่ใช้อธิบาย Kafka + โปรเจกต์ golang_kafka พร้อมความหมายแบบสั้น

---

## 1. โครงสร้างหลัก (Core)

| คำ | หมายถึง |
|---|---|
| **Topic** | หัวข้อ/ประเภทของ message แยกตามเรื่อง (เช่น `order`, `payment`) |
| **Partition** | เลนย่อยภายใน 1 topic — ซอยเพื่ออ่านขนานกันได้ + scale |
| **Offset** | เลขตำแหน่งของ message ในแต่ละ partition (0,1,2,…) นับแยกต่อ partition |
| **Message / Event** | ข้อมูล 1 ชิ้นที่ไหลผ่าน Kafka |
| **Payload** | "เนื้อ" ของข้อมูลใน message (เช่น JSON ของ order) = ส่วน Value |
| **Key** | ป้ายที่แปะมากับ message → `hash(key) % partition` ใช้เลือกว่าลง partition ไหน (key เดียวกันลงเลนเดิมเสมอ → รักษาลำดับ) |
| **Broker** | ตัวเซิร์ฟเวอร์ Kafka 1 เครื่อง (เก็บ partition + รับส่ง message) |
| **Cluster** | กลุ่มของ broker หลายเครื่องทำงานร่วมกัน |

## 2. ผู้ส่ง / ผู้รับ

| คำ | หมายถึง |
|---|---|
| **Producer** | ฝั่งที่ "ยิง" message เข้า topic (โค้ด application ของเรา) |
| **Consumer** | ฝั่งที่ "อ่าน" message จาก topic ไปประมวลผล |
| **Consumer Group** | กลุ่มของ consumer ที่ใช้ `group.id` เดียวกัน → แบ่ง partition กันอ่าน (1 partition = 1 consumer ในกลุ่ม) เป็นหน่วยที่ Kafka ใช้จำ offset |
| **Subscribe** | การที่ consumer บอกว่าจะอ่าน topic ไหนบ้าง |
| **Assignment** | partition ที่ Kafka มอบหมายให้ consumer ตัวนั้นดูแล |
| **Rebalance** | การที่ Kafka แจก partition ใหม่ให้สมาชิกในกลุ่ม (เกิดตอนมี consumer เพิ่ม/หาย) |

## 3. การจำตำแหน่ง / commit

| คำ | หมายถึง |
|---|---|
| **Commit** | การบันทึกว่า consumer group อ่าน/process ถึง offset ไหนแล้ว |
| **Committed offset** | "บุ๊กมาร์ก" ที่ปักไว้ — ค่าเดียวที่แปลว่า "ทุกตัวก่อนหน้านี้เสร็จแล้ว" |
| **auto.offset.reset** | จะเริ่มอ่านที่ไหนเมื่อ *ยังไม่มี* committed offset — `earliest` (ตัวแรกสุด) / `latest` (ตัวใหม่สุด) |
| **enable.auto.commit** | เปิด/ปิดการ commit อัตโนมัติ (default = true, commit ทุก ~5 วิ) |
| **Manual commit** | commit เองหลัง process เสร็จ — คุมได้ว่าจะ commit ตอนไหน |
| **Sequential / contiguous commit** | commit ได้แค่ offset ที่เสร็จ "ต่อเนื่องกันโดยไม่มีรูโหว่" เจอตัวแรกที่ยังไม่เสร็จก็หยุด |
| **__consumer_offsets** | topic พิเศษภายใน Kafka ที่เก็บ committed offset ของทุก group |

## 4. การันตีการส่ง (Delivery Semantics)

| คำ | หมายถึง |
|---|---|
| **at-least-once** | อ่านอย่างน้อย 1 ครั้ง — อาจซ้ำได้ แต่ไม่หาย (process ก่อน → commit ทีหลัง) |
| **at-most-once** | อ่านมากสุด 1 ครั้ง — ไม่ซ้ำ แต่อาจหาย (commit ก่อน → process ทีหลัง) |
| **exactly-once (EOS)** | ทำครั้งเดียวเป๊ะ ไม่ซ้ำไม่หาย — ต้องใช้ Kafka Transactions (scope จำกัดใน Kafka) |
| **Idempotent** | ทำซ้ำแล้วผลเหมือนเดิม ไม่พัง (หัวใจของ at-least-once) |
| **Idempotent consumer** | handler ที่เช็คก่อนทำ (เช่นเช็ค order_id ใน DB) → ทำซ้ำได้ปลอดภัย |
| **Idempotent producer** | `enable.idempotence=true` กัน producer ส่งซ้ำตอน retry |

## 5. ความทนทาน (Durability)

| คำ | หมายถึง |
|---|---|
| **Replication** | ก๊อป partition ไปเก็บอีก broker เพื่อสำรองกันเครื่องพัง |
| **Replication factor** | จำนวนสำเนาของแต่ละ partition (เช่น 3 = มี 3 ชุด) |
| **Leader / Replica** | leader = ตัวหลักที่รับ-ส่งจริง, replica = สำเนาสำรอง (พร้อมขึ้นแทนถ้า leader ตาย) |
| **ISR** (In-Sync Replicas) | สำเนาที่ตามข้อมูลทันกับ leader |
| **acks** | producer รอ ack แค่ไหน — `acks=all` = รอทุก replica ยืนยัน (ปลอดภัยสุด ไม่หาย) |

## 6. การเก็บข้อมูล (Retention / Storage)

| คำ | หมายถึง |
|---|---|
| **Retention** | นานแค่ไหนที่ Kafka เก็บ message ก่อนลบ — นับจาก *อายุ message* ไม่สนว่าอ่านยัง |
| **retention.ms** | retention ตามเวลา (default 7 วัน) |
| **retention.bytes** | retention ตามขนาด partition (โตเกินก็ลบตัวเก่าสุด) |
| **cleanup.policy** | `delete` (ลบตาม retention) / `compact` (เก็บค่าล่าสุดของแต่ละ key ไว้ตลอด) |
| **Log compaction** | โหมด compact — เก็บแต่ message ล่าสุดต่อ key (ใช้กับ state/snapshot) |
| **Segment** | ก้อนไฟล์ที่ partition แบ่งเก็บ — Kafka ลบทีละ segment เก่าสุด |

## 7. Pattern / สถาปัตยกรรม

| คำ | หมายถึง |
|---|---|
| **State Map** | โครงสร้างจำว่า offset ไหน process เสร็จแล้วบ้าง (ใช้กับ async + sequential commit) |
| **Dual write problem** | ปัญหาเขียน 2 ระบบ (DB + Kafka) ให้ atomic พร้อมกันไม่ได้ตรงๆ |
| **Transactional Outbox** | เขียน event ลงตาราง `outbox` ใน tx เดียวกับ business data → process แยกยิงเข้า Kafka ทีหลัง (แก้ dual write) |
| **Inbox Pattern** | เก็บ event_id ที่เคย process ลงตาราง `inbox` → เช็คก่อนทำ กัน process ซ้ำฝั่งรับ (คู่กับ outbox) — ดู `INBOX_PATTERN.md` |
| **CDC** (Change Data Capture) | จับทุกการเปลี่ยนแปลง (INSERT/UPDATE/DELETE) ในตาราง DB แล้ว stream เข้า Kafka อัตโนมัติ โดยแอปไม่ต้องยิงเอง — ดู `CDC_DEBEZIUM.md` |
| **WAL** (Write-Ahead Log) | log ลำดับทุกการเปลี่ยนแปลงที่ Postgres เขียนก่อน commit เสมอ — CDC แอบอ่าน WAL ตัวนี้เพื่อจับ change |
| **Logical replication** | ฟีเจอร์ Postgres แปลง WAL (binary) เป็น row-level change stream ผ่าน plugin `pgoutput` — ฐานของ CDC |
| **Replication slot** | "บุ๊กมาร์ก" ว่า CDC อ่าน WAL ถึงไหนแล้ว (กัน PG ลบ WAL ที่ยังไม่อ่าน — ถ้าค้างนานๆ disk เต็มได้) |
| **Debezium** | tool ทำ CDC ยอดนิยม รันเป็น connector บน Kafka Connect อ่าน WAL → ยิง change event เข้า Kafka |
| **Kafka Connect** | framework รัน connector (source/sink) จัดการ fault-tolerance/offset/scaling ให้ — Debezium รันบนนี้ |
| **Outbox Event Router** | pattern ลูกผสม: Debezium อ่านตาราง outbox แทน relay ที่เขียนเอง → ได้ business event + ไม่ต้อง maintain relay |
| **Saga** | จัดการ transaction ข้ามหลาย service ด้วยลำดับ event + compensating action |
| **Kafka Transactions / EOS** | transaction ภายใน Kafka (consume→process→produce + commit แบบ atomic) — ไม่ครอบ external DB |
| **Event-driven** | สถาปัตยกรรมที่ producer ยิง event → หลาย service react แยกกัน |

## 8. การ Deploy / Config (จาก kafka.yaml)

| คำ | หมายถึง |
|---|---|
| **ZooKeeper** | ระบบเก่าที่ Kafka เคยใช้จัดการ metadata (รุ่นใหม่เลิกใช้) |
| **KRaft** | โหมดใหม่ที่ Kafka จัดการ metadata เอง ไม่ต้องพึ่ง ZooKeeper |
| **bootstrap.servers** | ที่อยู่ broker ที่ client ใช้ต่อเข้า Kafka ครั้งแรก |
| **Listeners** | ช่องทาง/port ที่ Kafka เปิดรับการเชื่อมต่อ |
| **Advertised listeners** | ที่อยู่ที่ Kafka บอก client ให้ใช้ต่อกลับมา (host vs ใน docker network) |
| **PLAINTEXT** | โปรโตคอลแบบไม่เข้ารหัส (ใช้ใน dev) |
| **INTER_BROKER_LISTENER** | listener ที่ broker ใช้คุยกันเอง |
| **CONTROLLER listener** | listener ภายในของ KRaft (broker คุยกับ controller) |
| **PROCESS_ROLES** | บทบาทของ node ใน KRaft — `broker` / `controller` / ทั้งคู่ |
| **NODE_ID** | เลขประจำตัว node ใน cluster |
| **Controller quorum** | กลุ่ม controller ที่โหวตจัดการ metadata ใน KRaft |

## 9. ฝั่งโค้ด Go

| คำ | หมายถึง |
|---|---|
| **goroutine** | thread เบาๆ ของ Go ใช้รันงานขนานกัน |
| **channel** | ท่อส่งข้อมูลระหว่าง goroutine (โค้ดใช้ `msgCH` เชื่อม consumer ↔ handler) |
| **Delivery report** | event ที่ producer ส่งกลับมาบอกว่า message ส่งถึง Kafka สำเร็จ/ล้มเหลว |
| **ReadMessage timeout** | กรณีที่ไม่มี message ในช่วงนั้น (ไม่ใช่ error จริง — ต้องข้ามไป ไม่ใช่ panic) |
