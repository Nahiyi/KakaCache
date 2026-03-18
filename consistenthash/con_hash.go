package consistenthash

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map 一致性哈希实现
type Map struct {
	mu            sync.RWMutex
	config        *Config          // 配置信息
	keys          []int            // 哈希环索引切片，2^32个哈希索引
	hashMap       map[int]string   // 哈希环索引到节点的映射
	nodeReplicas  map[string]int   // 节点到虚拟节点数量的映射
	nodeCounts    map[string]int64 // 节点与节点负载（含虚拟）的映射，用于负载统计
	totalRequests int64            // 总请求数
}

// New 创建一致性哈希实例
func New(opts ...Option) *Map {
	// 先创建默认的Map
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
	}

	// 遍历配置项函数（允许有多个），依次执行配置
	for _, opt := range opts {
		opt(m)
	}

	m.startBalancer() // 启动负载均衡器
	return m
}

// Option 配置选项函数，拿到Map类型，并执行本函数进行自定义配置
type Option func(*Map)

// WithConfig 设置配置
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// Add 添加节点
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 遍历待添加的节点
	for _, node := range nodes {
		if node == "" {
			continue
		}

		// 为节点添加虚拟节点（节点是宏观笼统概括，具体就是指replicas数量个虚拟节点）
		// 所传入的node就是原始节点名称
		m.addNode(node, m.config.DefaultReplicas)
	}

	// 重新排序
	sort.Ints(m.keys)
	return nil
}

// Remove 移除节点
func (m *Map) Remove(node string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removeInternal(node)
}

// removeInternal 内部移除节点逻辑，调用方需持有互斥锁
func (m *Map) removeInternal(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	// 获取本节点的虚拟节点数量
	replicas, ok := m.nodeReplicas[node]
	if !ok {
		return fmt.Errorf("node %s not found", node)
	}

	// 遍历移除节点的所有虚拟节点
	for i := 0; i < replicas; i++ {
		// 和addNode时同理先计算出哈希环索引
		hashRingIdx := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		// 删除哈希环索引到该虚拟节点的映射
		delete(m.hashMap, hashRingIdx)
		// 在哈希环中移除该索引记录
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hashRingIdx {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	// 删除节点到虚拟节点数量的映射
	delete(m.nodeReplicas, node)
	// 删除节点到节点总负载的映射
	delete(m.nodeCounts, node)
	return nil
}

// Get 根据key获取对应路由到的节点
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	m.mu.RLock()
	if len(m.keys) == 0 {
		return ""
	}

	// 计算key的哈希环索引
	hash := int(m.config.HashFunc([]byte(key)))
	// 二分查找，找第一个让func(i)返回true的索引i
	idx := sort.Search(len(m.keys), func(i int) bool {
		// 返回第一个>=key的哈希环索引的位置，即顺时针最近节点
		return m.keys[i] >= hash
	})

	// 处理边界情况
	if idx == len(m.keys) {
		idx = 0
	}

	// 在读锁保护下直接获取节点名称，避免 RUnlock 后 keys 发生变化导致 idx 失效
	node := m.hashMap[m.keys[idx]] // m.keys[idx]就是哈希环索引
	m.mu.RUnlock()

	// 重新获取写锁更新统计数据
	if node != "" {
		m.mu.Lock()
		m.nodeCounts[node]++                 // 访问一次，负载+1
		atomic.AddInt64(&m.totalRequests, 1) // 总访问量+1
		m.mu.Unlock()
	}

	// [调试] 打印哈希计算结果
	logrus.Debugf("[ConsistentHash] Key: %s, Hash: %d, Idx: %d, Node: %s, TotalKeys: %d", key, hash, idx, node, len(m.keys))

	return node
}

// addNode 添加节点的虚拟节点
func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		// 根据为哈希环配置的哈希函数计算输出每个虚拟节点的key的哈希环索引
		hashRingIdx := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		// 添加本虚拟节点的索引到哈希环中（上行强转为int，可能溢出为负数，但是实际不影响！）
		m.keys = append(m.keys, hashRingIdx)
		// 添加一条哈希环索引-节点的映射
		m.hashMap[hashRingIdx] = node
	}
	// 遍历完毕后，添加一条节点-节点的虚拟节点数量的映射
	m.nodeReplicas[node] = replicas
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return // 样本太少，不进行调整
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// 计算负载情况：每个节点平均访问量
	avgLoad := float64(atomic.LoadInt64(&m.totalRequests)) / float64(len(m.nodeReplicas))
	var maxDiff float64
	// 取极差为负载不均衡度
	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}

	// 如果负载不均衡度超过阈值，调整虚拟节点
	if maxDiff > m.config.LoadBalanceThreshold {
		// 释放读锁，以便rebalanceNodes能获取写锁
		m.mu.RUnlock()
		m.rebalanceNodes()
		// 重新获取读锁，以配合defer RUnlock
		m.mu.RLock()
	}
}

// rebalanceNodes 重新平衡节点
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	// 调整每个节点的虚拟节点数量
	for node, count := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(count) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
			// 负载过高，减少虚拟节点
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 负载过低，增加虚拟节点
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 确保在限制范围内
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
			// 重新添加节点的虚拟节点
			// 使用内部方法避免死锁
			if err := m.removeInternal(node); err != nil {
				continue // 如果移除失败，跳过这个节点
			}
			m.addNode(node, newReplicas)
		}
	}

	// 重置计数器
	for node := range m.nodeCounts {
		m.nodeCounts[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	// 重新排序
	sort.Ints(m.keys)
}

// GetStats 获取负载统计信息
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer() {
	go func() {
		// 启动一个定时器，每秒向ticker.C管道发送时间
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop() // 直到goroutine终止，即程序退出

		for range ticker.C {
			// 定时检查并重新负载均衡
			m.checkAndRebalance()
		}
	}()
}
