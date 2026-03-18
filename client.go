package kakacache

import (
	"context"
	"fmt"
	"time"

	pb "github.com/Nahiyi/KakaCache/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client 实现了Peer接口，负责向指定的远程节点发起gRPC调用
type Client struct {
	addr    string             // 远程节点的地址
	svcName string             // 服务名
	conn    *grpc.ClientConn   // 底层的 TCP 连接
	grpcCli pb.KakaCacheClient // Protobuf 生成的客户端 Stub
}

// 编译期断言，确保 Client 实现了 Peer 接口
var _ Peer = (*Client)(nil)

func NewClient(addr string, svcName string) (*Client, error) {
	// 创建grpc连接
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}

	// 创建grpc客户端
	grpcClient := pb.NewKakaCacheClient(conn)

	client := &Client{
		addr:    addr,
		svcName: svcName,
		conn:    conn,
		grpcCli: grpcClient,
	}

	return client, nil
}

func (c *Client) Get(group, key string) ([]byte, error) {
	// 设置 3 秒超时控制
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 封装请求，并执行调用
	resp, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from kakacache: %v", err)
	}

	// 从响应中得到Value（[]byte类型）
	return resp.GetValue(), nil
}

func (c *Client) Delete(group, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete value from kakacache: %v", err)
	}

	return resp.GetValue(), nil
}

func (c *Client) Set(ctx context.Context, group, key string, value []byte) error {
	resp, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set value to kakacache: %v", err)
	}
	logrus.Infof("grpc set request resp: %+v", resp)

	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
