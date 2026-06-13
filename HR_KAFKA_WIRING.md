# HR → Kafka producer wiring (hr.projects)

> Scaffold วางไว้แล้ว: `internal/adapter/eventpub/` (outbox, project_producer, relay) + migration `0092_event_outbox.up.sql` + go.mod (require/replace events)
> เหลือ 3 จุดต่อด้วยมือ (ผมไม่แตะ main/repo/config โดยไม่ได้ compile) + 1 คำสั่ง tidy

## 0. ติดตั้ง dependency (จำเป็น — sandbox ไม่มี Go จึงยังไม่ได้รัน)
```bash
cd erp_hr_module_be
go mod tidy      # ดึง github.com/hap-erp/events (+ segmentio/kafka-go ผ่าน bus)
go build ./...   # ต้องผ่านก่อนไปต่อ
```

## 1. enqueue project event ใน tx เดียวกับ project write
`internal/adapter/postgres/project_repo.go` — `Create`/`Update` ตอนนี้ใช้ `r.pool.QueryRow(...)` ตรง (ไม่มี tx)
แก้ให้ห่อด้วย tx + enqueue (outbox pattern · atomic):

```go
import (
    "github.com/hap-erp/events"
    "github.com/hap-erp/hr-backend/internal/adapter/eventpub"
)

func (r *ProjectRepo) Create(ctx context.Context, in domain.CreateProjectInput) (domain.Project, error) {
    tx, err := r.pool.Begin(ctx)
    if err != nil { return domain.Project{}, err }
    defer tx.Rollback(ctx)

    p, err := scanProject(tx.QueryRow(ctx, `INSERT INTO projects (...) VALUES (...) RETURNING `+projectCols, ...))
    if err != nil { return domain.Project{}, err }

    if err := eventpub.ProjectCreated(ctx, tx, toProjectPayload(p)); err != nil {
        return domain.Project{}, err
    }
    return p, tx.Commit(ctx)
}
// Update → eventpub.ProjectUpdated · ปิดงาน (active=false) → eventpub.ProjectClosed
```
helper map domain → payload:
```go
func toProjectPayload(p domain.Project) events.ProjectPayload {
    return events.ProjectPayload{
        ProjectID: p.ID, Code: p.Code, Name: p.Name,
        Owner: p.Owner, Province: p.Province,
        ProjectCost: p.ProjectCost, Active: p.Active,
    }
}
```
> `eventpub.Execer` รับทั้ง `pgx.Tx` และ `*pgxpool.Pool` → ส่ง `tx` เข้าไปได้เลย

## 2. start relay ใน main
`cmd/api/main.go` — หลังสร้าง pool + อ่าน Kafka brokers จาก config:
```go
if pool != nil && len(cfg.KafkaBrokers) > 0 {
    publisher := bus.NewPublisher(cfg.KafkaBrokers)
    defer publisher.Close()
    rl := eventpub.NewRelay(pool, publisher, 2*time.Second)
    go rl.Run(ctx)
    log.Printf("kafka: hr outbox relay → %v", cfg.KafkaBrokers)
}
```
import: `"github.com/hap-erp/events/bus"` + `"github.com/hap-erp/hr-backend/internal/adapter/eventpub"`

## 3. เพิ่ม Kafka config
`internal/config/` — เพิ่ม `KafkaBrokers []string` อ่านจาก env `KAFKA_BROKERS` (split ","), และ `app.env`:
```
KAFKA_BROKERS=localhost:9094
```
(inventory ใช้ key เดียวกัน — ดู `erp_inventory_module_be/internal/config`)

## ผลลัพธ์ปลายทาง
HR สร้าง/แก้โครงการ → enqueue `hr.projects` (project.created/updated/closed) → relay ส่งเข้า Kafka
→ inventory `inventory-hr-projects-sync` consume → upsert `hr_projects` projection (มี scaffold ฝั่ง inventory แล้ว)

## ยังไม่ทำ (เฟสถัดไป)
- `user.*` producer ของ HR (hr.users) — โครง eventpub ใช้ซ้ำได้ แค่เพิ่ม user_producer.go + enqueue ตอน user mutation
- backfill snapshot ครั้งแรก (ก่อน event stream เริ่ม) ผ่าน HR_INTERNAL_URL
