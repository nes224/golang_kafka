# HAP ERP — End-to-End Workflow (As-Built) · HR → Warehouse → Engineer → Procurement

> **As-built** flow จริงที่ implement แล้ว (ไม่ใช่ target design) · อัปเดต: 2026-06-18
> design/target choreography ฉบับเต็ม = [`HAP_ERP_Kafka_Architecture_Design.md`](./HAP_ERP_Kafka_Architecture_Design.md) §15
> คอนเซปต์ EDA vs broker = [`EDA_VS_BROKER_NOTES.md`](./EDA_VS_BROKER_NOTES.md)

---

## 0. บทบาทแต่ละ module

| Module | port | เป็นเจ้าของ | ออก event | กิน event |
|---|---|---|---|---|
| **HR** | 8001 | users + projects + **JWT (auth กลาง)** | `hr.users` · `hr.projects` | — (publish-only) |
| **WAREHOUSE** (inventory) | 8003 | stock · items · MRF · receive/return · tools · **shortfall** | `inventory.shortfall` (+ `inventory.stock` · `inventory.requisitions`) | `procurement.po` · `hr.users` · `hr.projects` |
| **ENGINEER** | *(8003)* | **ไม่มี BE แยก** — เป็น role-view ของ warehouse (ใช้ inventory ตัวเดียวกัน) | — | — |
| **PROCUREMENT** | 8006 | vendor · PR · RFQ · PO · GR | `procurement.po` (`po.received`) | `inventory.shortfall` |

> auth = **HTTP sync** (JWT จาก HR + session validate) — **ไม่ผ่าน event** · event ใช้กับ domain data เท่านั้น

---

## 1. Event topics (as-built · จาก `topics.go`)

| topic | event type | source → consumer |
|---|---|---|
| `hr.users` | `user.created/updated/deactivated` | HR → inventory (sync `hr_users`) |
| `hr.projects` | `project.created/updated/closed` | HR → inventory (sync `hr_projects`) |
| `inventory.shortfall` | `shortfall.created` | **inventory → procurement** (auto-create PR) |
| `procurement.po` | `po.received` | **procurement → inventory** (auto-receipt) |

---

## 2. Golden path — ไล่ตามลำดับจริง

```
[HR] สร้าง user (วิศวกร/สโตร์) + project
       └─ hr.users / hr.projects ──► ทุก module mirror (projection)
       └─ ออก JWT ──► ทุก request ของทุก module verify กับ HR (session check)
   │
   ▼
[ENGINEER] เปิดใบเบิก MRF (ขอวัสดุเข้า project)
   │   inventory: requisition CREATE · status WAITING
   ▼
[WAREHOUSE / STORE] รับ MRF
   │   pick (WAITING→PICKING) → ship (PICKING→SHIPPED)
   │   → ISSUE stock movement (ของออกจากคลัง · allocate เข้า project ledger)
   │
   ├── ของพอทุก line ─────────────────────────────────────────────┐
   │                                                                │
   └── มี line SHORT (requested > picked)                          │
          │                                                         │
          ▼                                                         │
   [WAREHOUSE] shortfall aggregate (รวบ SHORT จาก MRF เปิด)         │
          │  POST /shortfalls/:id/publish                           │
          │  └─ publish ─inventory.shortfall.created──►             │
          │                                          │              │
          │                                          ▼              │
          │              [PROCUREMENT] consume → auto-create PR (pending · idempotent ด้วย source_shortfall_id)
          │                          │                              │
          │                          ▼                              │
          │              PR submit → RFQ (เทียบราคา vendor) → award │
          │                          → PO (ออกให้ vendor)           │
          │                          → vendor ส่ง → GR (รับของ)     │
          │                          → รับครบ                       │
          │                          └─ publish ─procurement.po.received──►
          │                                                  │      │
          │                                                  ▼      │
          │                  [WAREHOUSE] consume → auto-RECEIPT (stock + balance เพิ่ม)
          │                                                  │      │
          │                  มีของแล้ว → ship MRF เดิมต่อ ───┘      │
          │                                                         │
          ▼ ◄───────────────────────────────────────────────────── ┘
   [ENGINEER] receive (SHIPPED→DONE) → วัสดุพร้อมใช้
          • IMP (เบิกใช้หน้างาน) → ตัดจาก project ledger (event-log · ไม่แตะ stock คลัง)
          • Daily Report (รายงานประจำวัน) · Project Plan
          • Ledger = issued − used
```

**loop ย่อ:** Engineer ขอ → Warehouse ไม่มีของ → Procurement ซื้อ → ของกลับเข้า Warehouse (auto) → Warehouse ส่งต่อ Engineer → Engineer ใช้ · **HR เป็นฐาน** (auth + master data)

---

## 3. Step-by-step + endpoint (admin JWT)

| # | ใคร | action | endpoint | event ที่เกิด |
|---|---|---|---|---|
| 1 | HR | สร้าง project + user | `POST /api/projects` · user APIs | `hr.projects` · `hr.users` |
| 2 | Engineer | เปิด MRF | `POST /api/inventory/requisitions` | — (WAITING) |
| 3 | Store | จัดของ | `POST /api/inventory/requisitions/:id/pick` | — (PICKING) |
| 4 | Store | ส่งของ | `POST /api/inventory/requisitions/:id/ship` | ISSUE (SHIPPED) |
| 5 | Store | *(ถ้าของขาด)* รวบ shortfall | `GET /api/inventory/shortfalls/aggregate` → `POST /api/inventory/shortfalls` | — (OPEN) |
| 6 | Store | ส่งไปจัดซื้อ | `POST /api/inventory/shortfalls/:id/publish` | **`inventory.shortfall.created`** ◄ event แรกวิ่ง |
| 7 | *(auto)* | procurement สร้าง PR | *(consumer)* | — (PR pending) |
| 8 | Procurement | submit PR → RFQ → PO | `POST /api/pr/:id/submit` · `/api/rfq` · `/api/po` | — |
| 9 | Procurement | รับของ (GR) ครบ | `POST /api/po/:id/receive` | **`procurement.po.received`** |
| 10 | *(auto)* | inventory auto-receipt | *(consumer)* | stock + balance เพิ่ม |
| 11 | Engineer | receive MRF | `POST /api/inventory/requisitions/:id/receive` | — (DONE) |
| 12 | Engineer | เบิกใช้ | `POST /api/inventory/implementations` | — (IMP) |

> ⚠️ ขั้น 9→10: `po.received` auto-receipt บวกสต็อกเฉพาะ line ที่มี **`item_id`** (PR จาก shortfall พก item_id มาให้) · PR manual ไม่มี item_id → ไม่ auto-receipt

---

## 4. Dev runbook — ไล่/รัน E2E ยังไง

### 4.1 Infra
```bash
# kafka (event bus กลาง) — ครั้งเดียว
cd erp_kafka_module && docker compose -f docker-compose.dev.kafka.yml up -d
# kafka-ui ดู message: http://localhost:8085   ·   broker (host) = localhost:9094
# Postgres: แต่ละ module มี DB ของตัวเอง (รัน migrate-up ให้ครบ)
```

### 4.2 เปิด event ให้แต่ละ BE (dev) — `app.env.local` (gitignored)
```bash
EVENT_TRANSPORT=kafka
KAFKA_BROKERS=localhost:9094
# inventory เพิ่ม (สำหรับ po.received auto-receipt):
INVENTORY_SYSTEM_USER_ID=<user id ที่จะเป็น "ผู้รับ">
```
> มีให้แล้ว: inventory + procurement · **ยังไม่มี: HR** (เพิ่ม one-liner เดียวกันถ้าอยากให้ `hr.*` วิ่ง)

### 4.3 รัน BE (แต่ละ repo · air หรือ go run)
```
HR (8001)  →  inventory (8003)  →  procurement (8006)
```

### 4.4 Trigger + ไล่ดู
- **FE ยังไม่ wire** → ยิงผ่าน **curl + admin JWT** (login HR เอา token) ตามตาราง §3
- ดู event วิ่งที่ **kafka-ui :8085** → topic `inventory.shortfall` → `procurement.po`
- ตรวจผล: PR ถูก auto-create ฝั่ง procurement · stock เพิ่มฝั่ง inventory หลัง `po.received`

---

## 5. สถานะ as-built + ตัวบล็อก E2E เต็ม

| | สถานะ |
|---|---|
| event bus (kafka/pubsub abstraction) | ✅ พร้อม · สลับด้วย `EVENT_TRANSPORT` |
| inventory: MRF · shortfall publish · po.received consumer | ✅ (consumer ทดสอบ live ยังไม่ครบ) |
| procurement: shortfall consumer (auto-PR) · PR/RFQ/PO/GR · po.received publish | ✅ scaffolded + DB-verified |
| HR: users/projects publisher | ✅ (เป็น source ปัจจุบัน) |
| **RBAC seed** (`inventory_*` · `purchasing_*`) | ⚠️ ยังไม่ครบ → ใช้ **admin (`*`)** test เท่านั้น |
| **FE wiring** (purchasing/warehouse/engineer → BE) | ❌ ยังไม่ทำ → ไล่ผ่าน curl/kafka-ui |
| `INVENTORY_SYSTEM_USER_ID` | ⚠️ ต้องตั้งก่อน test `po.received` auto-receipt |
| projects single-source (inventory = source) | 🔜 PROPOSED ([`docs/PROJECTS_SINGLE_SOURCE_MIGRATION.md`](../erp_inventory_module_be/docs/PROJECTS_SINGLE_SOURCE_MIGRATION.md)) |

---

## 6. related docs
| เรื่อง | doc |
|---|---|
| design/target choreography | `HAP_ERP_Kafka_Architecture_Design.md` §14-15 |
| EDA vs broker (คอนเซปต์) | `EDA_VS_BROKER_NOTES.md` |
| outbox/inbox/relay + replica-safety | `DEPLOYMENT_TOPOLOGY.md` |
| inventory↔procurement bridge | `erp_inventory_module_be/docs/WORKFLOW_ANALYSIS.md` · `API.md` |
| HR↔warehouse integration | `erp_hr_module_be/docs/WAREHOUSE_INTEGRATION.md` |
| prod transport (Pub/Sub) | `erp_inventory_module_be/docs/PUBSUB_SETUP.md` |
