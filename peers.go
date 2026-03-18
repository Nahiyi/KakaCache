package kakacache

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Nahiyi/KakaCache/consistenthash"
	"github.com/Nahiyi/KakaCache/registry"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "kaka-cache"

// PeerPicker 定义了peer选择器的接口
// 它的职责是：根据传入的 key，返回负责该 key 的远端节点
type PeerPicker interface {
	// PickPeer 根据 key 选择节点
	// 返回值:
	// peer: 获取到的远端节点的客户端（接口）
	// ok: 是否成功找到了节点 (如果集群只有自己一个节点，或者都没上线，返回 false)
	// self: 找到的节点是不是当前节点自己
	PickPeer(key string) (peer Peer, ok bool, self bool)

	// Close 关闭选择器
	Close() error
}

// Peer 定义了缓存节点的接口
// 它的职责是：作为客户端，向远端的物理节点发起网络请求
type Peer interface {
	// Get 向远端节点请求获取某个 Group 下的 Key 的值
	Get(group string, key string) ([]byte, error)

	// Set 向远端节点发送写入请求 (通常是内部同步)
	Set(ctx context.Context, group string, key string, value []byte) error

	// Delete 向远端节点发送删除请求 (通常是内部同步)
	Delete(group string, key string) (bool, error)

	// Close 关闭该远端节点的连接
	Close() error
}

// ClientPicker 实现了PeerPicker接口，是基于 Etcd 和一致性哈希的具体实现
type ClientPicker struct {
	selfAddr string              // 当前节点自己的地址，防止自己向自己发起RPC
	svcName  string              // 服务名称，对应Etcd中的注册前缀
	mu       sync.RWMutex        // 读写锁，保护哈希环和客户端映射
	consHash *consistenthash.Map // 一致性哈希环
	clients  map[string]*Client  // 维护了每一个节点地址到 Client 实例的映射
	etcdCli  *clientv3.Client    // Etcd 客户端
	ctx      context.Context     // 上下文，用于控制后台监听协程的退出
	cancel   context.CancelFunc  // 取消函数
}

// PickerOption 定义配置选项模式
type PickerOption func(*ClientPicker)

// WithServiceName 设置服务名称
func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

// PrintPeers 打印当前已发现的节点（仅用于调试）
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("当前已发现的节点:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

// NewClientPicker 创建新的ClientPicker实例并启动服务发现
func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		svcName:  defaultSvcName,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
		ctx:      ctx,
		cancel:   cancel,
	}

	for _, opt := range opts {
		opt(picker)
	}

	// 关键debug：把“自己”也加入到一致性哈希环中！
	// 如果自己不在环里，自己计算哈希时就永远不可能命中自己，只会命中别人（因此极可能导致死锁情况，最终百分百超时）
	picker.consHash.Add(addr)

	// 初始化 Etcd 客户端，准备向 Etcd 请求数据
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdCli = cli

	// 启动服务发现机制 (全量拉取 + 增量监听)
	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}

	return picker, nil
}

// startServiceDiscovery 启动服务发现
func (p *ClientPicker) startServiceDiscovery() error {
	// 先进行一次全量更新，把当前 Etcd 里已有的服务节点全拉下来
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	// 启动一个后台协程，进行增量更新（监听节点上下线）
	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听服务实例变化
func (p *ClientPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdCli)
	// 监听特定前缀，比如 "/services/kaka-cache"
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		case <-p.ctx.Done(): // 收到关闭信号
			watcher.Close()
			return
		case resp := <-watchChan: // 收到 Etcd 推送的事件
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理监听到的上下线事件
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, event := range events {
		// Etcd 中存的 value 就是节点的地址
		addr := string(event.Kv.Value)
		// 忽略自己
		if addr == p.selfAddr {
			continue
		}

		switch event.Type {
		case clientv3.EventTypePut:
			// 有新节点上线，或者节点信息更新
			if _, exists := p.clients[addr]; !exists {
				p.set(addr) // 添加到哈希环并建立连接
				logrus.Infof("New service discovered at %s", addr)
			}
		case clientv3.EventTypeDelete:
			// 有节点下线 (租约到期或主动撤销)
			if client, exists := p.clients[addr]; exists {
				client.Close() // 断开 gRPC 连接
				p.remove(addr) // 从哈希环移除
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

// fetchAllServices 全量获取所有服务实例
func (p *ClientPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get all services: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr != "" && addr != p.selfAddr {
			p.set(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}
	return nil
}

// set 添加服务实例：创建 Client，并添加到一致性哈希环
func (p *ClientPicker) set(addr string) {
	// 调用 client.go 里的 NewClient 建立 gRPC 连接
	if client, err := NewClient(addr, p.svcName); err == nil {
		p.consHash.Add(addr)     // 添加到哈希环
		p.clients[addr] = client // 存入缓存字典
		logrus.Infof("Successfully created client for %s", addr)
	} else {
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
	}
}

// remove 移除服务实例：从哈希环和缓存字典中移除
func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr)
	delete(p.clients, addr)
}

// PickPeer 核心路由方法：根据 key 找到负责该 key 的目标节点
func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 使用一致性哈希算法计算出目标节点地址
	if addr := p.consHash.Get(key); addr != "" {
		if client, ok := p.clients[addr]; ok {
			// 返回目标 Client, 是否成功, 是否是自己
			return client, true, addr == p.selfAddr
		}
	}
	return nil, false, false
}

// Close 关闭选择器，清理所有资源
func (p *ClientPicker) Close() error {
	p.cancel() // 停止 watch 协程
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	// 断开所有已经建立的 gRPC 客户端连接
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	// 关闭 Etcd 客户端
	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

// parseAddrFromKey 从etcd key中解析地址（辅助方法）
func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}
