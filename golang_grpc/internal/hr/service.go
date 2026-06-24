// Package hr — implement HRService (ฝั่ง server)
// ใช้ mock data ใน memory แทน DB จริง เพื่อโฟกัสที่กลไก gRPC
package hr

import (
	"context"
	"log"
	"sync"

	"github.com/golang_grpc/proto/hrpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service ต้อง embed UnimplementedHRServiceServer (generated)
// เพื่อรองรับ method ใหม่ในอนาคตโดยไม่พัง (forward compatible)
type Service struct {
	hrpb.UnimplementedHRServiceServer

	mu        sync.RWMutex
	employees map[string]*hrpb.Employee
	sessions  map[string]string // sessionID -> employeeID
}

func NewService() *Service {
	employees := map[string]*hrpb.Employee{
		"E001": {Id: "E001", FirstName: "เมฆ", LastName: "วิชยสาตร์", Department: "Engineering", JobTitle: "Lead Engineer", Roles: []string{"engineer", "admin"}},
		"E002": {Id: "E002", FirstName: "สมชาย", LastName: "ใจดี", Department: "Warehouse", JobTitle: "Warehouse Officer", Roles: []string{"warehouse"}},
		"E003": {Id: "E003", FirstName: "สมหญิง", LastName: "รักงาน", Department: "Procurement", JobTitle: "Procurement Officer", Roles: []string{"procurement"}},
	}
	sessions := map[string]string{
		"sess-eng-001":   "E001",
		"sess-wh-002":    "E002",
		"sess-proc-003":  "E003",
		// sess อื่นๆ ที่ไม่มีในนี้ = หมดอายุ/ไม่ valid
	}
	return &Service{employees: employees, sessions: sessions}
}

// ValidateSession — RPC ตัวที่ 1
// signature ตรงตาม interface ที่ generated มาจาก .proto เป๊ะ
func (s *Service) ValidateSession(ctx context.Context, req *hrpb.ValidateSessionRequest) (*hrpb.ValidateSessionResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("[HR] ValidateSession session_id=%s", req.SessionId)

	empID, ok := s.sessions[req.SessionId]
	if !ok {
		// ไม่เจอ = session ไม่ valid · ไม่ใช่ error · ตอบ active=false
		return &hrpb.ValidateSessionResponse{Active: false}, nil
	}
	emp := s.employees[empID]
	return &hrpb.ValidateSessionResponse{
		Active:      true,
		EmployeeId:  empID,
		Roles:       emp.Roles,
		Permissions: permsForRoles(emp.Roles),
	}, nil
}

// GetEmployee — RPC ตัวที่ 2
func (s *Service) GetEmployee(ctx context.Context, req *hrpb.GetEmployeeRequest) (*hrpb.Employee, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("[HR] GetEmployee employee_id=%s", req.EmployeeId)

	emp, ok := s.employees[req.EmployeeId]
	if !ok {
		// gRPC มี status code มาตรฐาน (เทียบ HTTP 404) → client เช็คได้ตรงๆ
		return nil, status.Errorf(codes.NotFound, "employee %s not found", req.EmployeeId)
	}
	return emp, nil
}

// permsForRoles — แปลง role → permission (mock business logic)
func permsForRoles(roles []string) []string {
	table := map[string][]string{
		"admin":       {"*"},
		"engineer":    {"project.read", "project.write", "drawing.read"},
		"warehouse":   {"stock.read", "stock.write", "requisition.read"},
		"procurement": {"po.read", "po.write", "vendor.read"},
	}
	seen := map[string]bool{}
	var out []string
	for _, r := range roles {
		for _, p := range table[r] {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return out
}
