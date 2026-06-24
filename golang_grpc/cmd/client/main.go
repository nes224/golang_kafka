// Client — ใช้แทน Engineer / Warehouse / Procurement ที่ยิงเข้าไปถาม HR
// เลือกว่าใครเรียกด้วย flag --caller · ใส่ session ด้วย --session
//
// ตัวอย่าง:
//   go run ./cmd/client --caller WAREHOUSE  --session sess-wh-002
//   go run ./cmd/client --caller ENGINEER   --session sess-eng-001
//   go run ./cmd/client --caller PROCUREMENT --session sess-bad-999   (session ปลอม)
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/golang_grpc/proto/hrpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	caller := flag.String("caller", "WAREHOUSE", "service ที่เรียก (ENGINEER/WAREHOUSE/PROCUREMENT)")
	session := flag.String("session", "sess-wh-002", "session id ที่จะตรวจ")
	addr := flag.String("addr", "localhost:50051", "ที่อยู่ HR gRPC server")
	flag.Parse()

	// 1) ต่อไปยัง HR server (insecure = ไม่เข้ารหัส เพราะ demo local)
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s] connect: %v", *caller, err)
	}
	defer conn.Close()

	// 2) สร้าง client stub จาก generated code — เรียก method ได้เหมือนฟังก์ชัน local
	client := hrpb.NewHRServiceClient(conn)

	// ctx + timeout 3 วิ (ถ้า HR ช้า/ล่ม ไม่รอเกิน 3 วิ) — context ที่เพิ่งเรียนมา!
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// ---- เรียก RPC ตัวที่ 1: ValidateSession ----
	vr, err := client.ValidateSession(ctx, &hrpb.ValidateSessionRequest{SessionId: *session})
	if err != nil {
		log.Fatalf("[%s] ValidateSession error: %v", *caller, err)
	}
	if !vr.Active {
		log.Printf("[%s] ❌ session ไม่ valid (%s) → ปฏิเสธ request", *caller, *session)
		return
	}
	log.Printf("[%s] ✅ session OK · emp=%s · roles=%v · perms=%v",
		*caller, vr.EmployeeId, vr.Roles, vr.Permissions)

	// ---- เรียก RPC ตัวที่ 2: GetEmployee ----
	emp, err := client.GetEmployee(ctx, &hrpb.GetEmployeeRequest{EmployeeId: vr.EmployeeId})
	if err != nil {
		// gRPC error มี status code → เช็คได้ว่าเป็น NotFound หรืออื่นๆ
		st := status.Convert(err)
		log.Fatalf("[%s] GetEmployee error: code=%s msg=%s", *caller, st.Code(), st.Message())
	}
	log.Printf("[%s] 👤 %s %s · %s · %s",
		*caller, emp.FirstName, emp.LastName, emp.Department, emp.JobTitle)
}
