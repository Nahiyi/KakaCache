package registry

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config 定义etcd客户端配置
type Config struct {
	Endpoints   []string      // Etcd集群地址列表
	DialTimeout time.Duration // 连接超时时间
}

// DefaultConfig 提供默认的 Etcd 连接配置
var DefaultConfig = &Config{
	Endpoints:   []string{"localhost:2379"}, // 默认连接本地2379端口
	DialTimeout: 5 * time.Second,
}

// Register 注册服务到etcd
// svcName: 服务名称 (如 "NodeA")
// addr: 当前节点的地址 (如 "127.0.0.1:8001")
// stopCh: 停止信号通道，当收到该信号时，主动撤销注册
func Register(svcName, addr string, stopCh <-chan error) error {
	// 创建 etcd 客户端连接
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   DefaultConfig.Endpoints,
		DialTimeout: DefaultConfig.DialTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 自动补全IP地址（如果传入的addr只有端口号如":8001"）
	if addr[0] == ':' {
		localIP, err := getLocalIP()
		if err != nil {
			cli.Close()
			return fmt.Errorf("failed to get local IP: %v", err)
		}
		addr = fmt.Sprintf("%s%s", localIP, addr)
	}

	// 创建租约(Lease)
	// 租约的作用是：如果我宕机了，没法主动删掉自己的信息，Etcd会在租约到期后自动删除我；默认设置10s
	lease, err := cli.Grant(context.Background(), 10)
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to create lease: %v", err)
	}

	// 将自己的地址注册到Etcd，并绑定刚才的租约
	// Key 的格式类似: /services/NodeA/127.0.0.1:8001
	key := fmt.Sprintf("/services/%s/%s", svcName, addr)
	_, err = cli.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to put key-value to etcd: %v", err)
	}

	// 开启自动续租(KeepAlive保活)
	// 因为我不想10秒后被踢出，所以只要节点还活着，就不断向Etcd发送心跳续租保活
	keepAliveCh, err := cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to keep lease alive: %v", err)
	}

	// 启动一个后台goroutine监听状态
	go func() {
		defer cli.Close()
		for {
			select {
			case <-stopCh:
				// 收到停止信号(Server关闭时触发)，主动撤销租约，优雅下线
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				cli.Revoke(ctx, lease.ID)
				cancel() // 三秒内已经成功撤约，提前结束ctx
				return
			case resp, ok := <-keepAliveCh:
				// 如果续租通道被关闭，说明和Etcd的连接断了或者出问题了
				if !ok {
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()

	logrus.Infof("Service registered: %s at %s", svcName, addr)
	return nil
}

// getLocalIP 获取当前机器的本地非回环IP地址
func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP found")
}
