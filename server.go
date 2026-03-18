package kakacache

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/Nahiyi/KakaCache/pb"
	"github.com/Nahiyi/KakaCache/registry" // 注册中心包
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Server 定义缓存服务器
type Server struct {
	pb.UnimplementedKakaCacheServer
	addr       string         // 服务地址
	svcName    string         // 服务名称
	groups     *sync.Map      // 缓存组
	grpcServer *grpc.Server   // gRPC服务器
	stopCh     chan error     // 停止信号
	opts       *ServerOptions // 服务器配置选项 (保留，体现工程规范)
}

// ServerOptions 服务器配置选项结构体
type ServerOptions struct {
	EtcdEndpoints []string      // Etcd 集群地址
	DialTimeout   time.Duration // 连接超时时间
	MaxMsgSize    int           // gRPC 最大消息大小
	TLS           bool          // 是否启用 TLS
	CertFile      string        // 证书文件路径
	KeyFile       string        // 密钥文件路径
}

// DefaultServerOptions 默认配置
var DefaultServerOptions = &ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

// ServerOption 定义函数选项模式的类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置 Etcd 端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// WithMaxMsgSize 设置最大消息大小
func WithMaxMsgSize(size int) ServerOption {
	return func(o *ServerOptions) {
		o.MaxMsgSize = size
	}
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	// 应用配置选项
	options := DefaultServerOptions
	for _, opt := range opts {
		opt(options)
	}

	// 初始化 gRPC 服务器选项
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...),
		stopCh:     make(chan error), // 初始化管道，防止 Start 中出现空指针
		opts:       options,
	}

	// 注册 gRPC 服务
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

	// 注册到 etcd 的逻辑
	go func() {
		// registry.Register注册成功直接结束本协程
		if err := registry.Register(s.svcName, s.addr, s.stopCh); err != nil {
			logrus.Errorf("failed to register service: %v", err)
			close(s.stopCh)
			return
		}
	}()

	logrus.Infof("Server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务器
func (s *Server) Stop() {
	// 关闭 stopCh，通知 registry.Register 中的协程撤销租约
	close(s.stopCh)
	s.grpcServer.GracefulStop()
}

// Get 实现Cache服务的Get方法
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	logrus.Debugf("[Server] 收到 gRPC Get 请求 -> Group: %s, Key: %s", req.Group, req.Key)
	// 根据组名获取对应的缓存组（由于是全局变量，同包直接调用即可）
	group := GetGroup(req.Group)
	if group == nil {
		logrus.Errorf("[Server] Group 不存在 -> %s", req.Group)
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	// 调用group层的Get方法，得到缓存值字节视图
	view, err := group.Get(ctx, req.Key)
	if err != nil {
		logrus.Errorf("[Server] 获取缓存失败 -> Key: %s, 错误: %v", req.Key, err)
		return nil, err
	}

	logrus.Debugf("[Server] 成功返回数据 -> Key: %s, 大小: %d", req.Key, view.Len())
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

// loadTLSCredentials 加载TLS证书
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}), nil
}
