package KakaCache

import (
	"context"
	"fmt"
	"net"
	"sync"

	pb "KakaCache/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Server 定义缓存服务器
type Server struct {
	pb.UnimplementedKakaCacheServer
	addr       string       // 服务地址
	svcName    string       // 服务名称
	groups     *sync.Map    // 缓存组
	grpcServer *grpc.Server // gRPC服务器
	stopCh     chan error   // 停止信号
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string) (*Server, error) {
	var serverOpts []grpc.ServerOption

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...),
		stopCh:     make(chan error),
	}

	// 注册rpc服务
	pb.RegisterKakaCacheServer(srv.grpcServer, srv)

	// 注册健康检查服务
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	// 启动gRPC服务器
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 阶段三暂时省略注册到 etcd 的逻辑
	// go func() {
	// 	if err := registry.Register(s.svcName, s.addr, stopCh); err != nil {
	// 		logrus.Errorf("failed to register service: %v", err)
	// 		close(stopCh)
	// 		return
	// 	}
	// }()

	logrus.Infof("Server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务器
func (s *Server) Stop() {
	close(s.stopCh)
	s.grpcServer.GracefulStop()
}

// Get 实现Cache服务的Get方法
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	// 根据组名获取对应的缓存组（由于是全局变量，同包直接调用即可）
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	// 调用group层的Get方法，得到缓存值字节视图
	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	// 封装响应，唯一字段： Value
	return &pb.ResponseForGet{Value: view.ByteSLice()}, nil
}

// Set 实现Cache服务的Set方法
func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	// 从context中获取来源标记（客户端调用时基于WithValue的context实现的）
	fromPeer := ctx.Value("from_peer")
	if fromPeer == nil {
		// 如果没有则默认是peer调用
		ctx = context.WithValue(ctx, "from_peer", true)
	}

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete 实现Cache服务的Delete方法
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: err == nil}, err
}
