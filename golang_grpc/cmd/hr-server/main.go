// HR service — gRPC server · ฟังที่ :50051 รอ Engineer/Warehouse/Procurement ยิงเข้ามา
package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang_grpc/internal/hr"
	"github.com/golang_grpc/proto/hrpb"
	"google.golang.org/grpc"
)

func main() {
	addr := ":50051"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// สร้าง gRPC server แล้วลงทะเบียน service ของเรา
	grpcServer := grpc.NewServer()
	hrpb.RegisterHRServiceServer(grpcServer, hr.NewService())

	// graceful shutdown — รอ Ctrl+C แล้วปิดสะอาด
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("[HR] shutting down...")
		grpcServer.GracefulStop()
	}()

	log.Printf("[HR] gRPC server listening on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
