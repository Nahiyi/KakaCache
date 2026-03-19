package store

import (
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

// lru2Store 是 LRU-2 算法的顶级缓存管理器
// 它内部通过分片(sharding/buckets)和分段锁(桶级别)来支持高并发
// 每个分片维护一个一级缓存(二级索引[0])和一个二级缓存(二级索引[1])
type lru2Store struct {
	locks       []sync.Mutex                  // 分段锁，每个桶一个锁，减小锁粒度，极大提高并发量
	caches      [][2]*cache                   // 核心封装：caches[桶索引][级别]，级别0是一级缓存，1是二级缓存
	onEvicted   func(key string, value Value) // 全局淘汰回调
	cleanupTick *time.Ticker                  // 用于定期清理过期键的定时器
	mask        int32                         // 哈希掩码，用于路由且快速计算桶索引，替代取模操作
}

func newLRU2Cache(opts Options) *lru2Store {
	// 初始化桶数量
	if opts.BucketCount == 0 {
		opts.BucketCount = 16
	}
	// 每个桶的容量
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024
	}
	// 二级缓存的容量
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}

	mask := maskOfNextPowOf2(opts.BucketCount)
	s := &lru2Store{
		locks:       make([]sync.Mutex, mask+1),
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTick: time.NewTicker(opts.CleanupInterval),
		mask:        int32(mask),
	}

	for i := range s.caches {
		s.caches[i][0] = Create(opts.CapPerBucket)
		s.caches[i][1] = Create(opts.Level2Cap)
	}

	if opts.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// Get 从缓存中获取一个值
// 这里实现了 LRU-2 算法的核心晋升逻辑：
// 1. 先查一级缓存：若命中，说明被再次访问（满足频率大于1），将其从一级缓存删除，并晋升到二级缓存头部
// 2. 若一级没命中，再查二级缓存：若命中，内部会将其移动到二级缓存头部（刷新 LRU 顺序）
func (s *lru2Store) Get(key string) (Value, bool) {
	// 路由：先根据key，哈希得到一个索引
	idx := hashBKRD(key) & s.mask
	// 锁住对应的桶（锁的idx和桶的idx是一一对应）
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	// 获取当前时刻
	currentTime := Now()

	// 首先检查一级缓存
	n1, status1, expireAt := s.caches[idx][0].del(key)
	if status1 > 0 {
		// 从一级缓存找到项目
		if expireAt > 0 && currentTime >= expireAt {
			// 项目已过期，删除它
			s.delete(key, idx)
			logrus.Debugf("LRU2: 键 [%s] 在一级缓存中找到，但已过期被删除", key)
			return nil, false
		}

		// 项目有效，将其移至二级缓存
		s.caches[idx][1].put(key, n1.v, expireAt, s.onEvicted)
		logrus.Debugf("LRU2: 键 [%s] 在一级缓存命中，晋升至二级缓存", key)
		return n1.v, true
	}

	// 一级缓存未找到，检查二级缓存
	n2, status2 := s._get(key, idx, 1)
	if status2 > 0 && n2 != nil {
		if n2.expireAt > 0 && currentTime >= n2.expireAt {
			// 项目已过期，删除它
			s.delete(key, idx)
			logrus.Debugf("LRU2: 键 [%s] 在二级缓存中找到，但已过期被删除", key)
			return nil, false
		}

		return n2.v, true
	}

	return nil, false
}

// Set 插入一个键值对，默认永不过期
// 根据 LRU-2 算法，新插入的数据总是先放入【一级缓存】(级别 0)
func (s *lru2Store) Set(key string, value Value) error {
	return s.SetWithExpiration(key, value, 9999999999999999)
}

// SetWithExpiration 插入带有过期时间的键值对
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// 计算过期时间，确保单位一致
	expireAt := int64(0)
	if expiration > 0 {
		// now() 返回纳秒时间戳，确保 expiration 也是纳秒单位
		expireAt = Now() + int64(expiration.Nanoseconds())
	}

	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	// 双缓存重键可选保险优化
	// 如果这个 key 已经在二级缓存（热点区）中了，直接更新二级缓存，
	// 而非把它重新塞进一级缓存（会导致一二级同时存在这个 key 的数据不一致）
	/*if _, ok := s.caches[idx][1].hmap[key]; ok {
		s.caches[idx][1].put(key, value, expireAt, s.onEvicted)
		return nil
	}*/

	// 否则（二级没有），我们才把它放入一级缓存
	// （如果一级有，底层的 put 会自动做原地更新；如果一级没有，底层 put 会分配新节点）
	s.caches[idx][0].put(key, value, expireAt, s.onEvicted)

	return nil
}

// Delete 实现Store接口
func (s *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	return s.delete(key, idx)
}

// Clear 实现Store接口
func (s *lru2Store) Clear() {
	var keys []string

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			keys = append(keys, key)
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			// 检查键是否已经收集（避免重复）
			for _, k := range keys {
				if key == k {
					return true
				}
			}
			keys = append(keys, key)
			return true
		})

		s.locks[i].Unlock()
	}

	for _, key := range keys {
		s.Delete(key)
	}

	// s.expirations = sync.Map{}
}

// Len 实现Store接口
func (s *lru2Store) Len() int {
	count := 0

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})

		s.locks[i].Unlock()
	}

	return count
}

// Close 关闭缓存相关资源
func (s *lru2Store) Close() {
	if s.cleanupTick != nil {
		s.cleanupTick.Stop()
	}
}

// 内部时钟机制：用于极大地减少 time.Now() 造成的系统调用开销和对象逃逸导致的 GC 压力
// 通过后台协程每 100ms 更新一次全局变量 clock，业务代码只需进行原子读取(atomic.LoadInt64)即可获取当前时间
//
// 另外，这里将常量 prev (prev，前驱) 和 next (next，后继) 一并定义为包级变量
// prev = 0 代表 dlnk 数组中的第 0 个元素（前驱指针）
// next = 1 代表 dlnk 数组中的第 1 个元素（后继指针）
// 这样在调用 adjust(idx, prev, next) 时代码更加语义化
var clock, prev, next = time.Now().UnixNano(), uint16(0), uint16(1)

// Now 返回 clock 变量的当前值；atomic.LoadInt64 是原子操作，用于保证在多线程/协程环境中安全地读取 clock 变量的值
func Now() int64 { return atomic.LoadInt64(&clock) }

// init 初始化时钟
func init() {
	go func() {
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond)) // 保持 clock 在一个精确的时间范围内，同时避免频繁的系统调用
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// 实现了 BKDR 哈希算法，用于计算键的哈希值
func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// maskOfNextPowOf2 计算大于或等于输入值的最近 2 的幂次方减一作为掩码值，用于高效率取模运算
func maskOfNextPowOf2(cap uint16) uint16 {
	if cap > 0 && cap&(cap-1) == 0 {
		return cap - 1
	}

	// 通过多次右移和按位或操作，将二进制中最高的 1 位右边的所有位都填充为 1
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
}

type node struct {
	k        string
	v        Value
	expireAt int64 // 过期时间戳，expireAt = 0 表示已删除
}

// cache 是底层无指针双向链表的 LRU 实现
// 核心优化：避免使用常规的指针对象，转而使用数组/切片（m），大幅降低 Go GC 的扫描压力
type cache struct {
	// dlnk[0] 是哨兵节点：
	// dlnk[0][0] (即 dlnk[0][prev]) 存储链表尾部元素的索引
	// dlnk[0][1] (即 dlnk[0][next]) 存储链表头部元素的索引
	// dlnk[i] 代表第 i 个节点的 prev(dlnk[i][0]) 和 next(dlnk[i][1])
	dlnk [][2]uint16       // 模拟双向链表的索引数组，0 表示 prev(前驱)，1 表示 next(后继)
	m    []node            // 预先分配好的连续内存，用于存储实际的节点数据 (k, v, expireAt)
	hmap map[string]uint16 // 哈希表：存储键(key)到 m 数组索引(index)的映射，实现 O(1) 查找
	last uint16            // 记录 m 数组中已分配元素的个数，代表下一个新分配节点的索引位置
}

func Create(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, cap+1),
		m:    make([]node, cap),
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

// put 向缓存中添加项，如果是新增返回 1，更新返回 0
// 新添加或更新的项会被调整到链表头部(最近使用)
// 如果容量已满，则触发 LRU 淘汰机制：复用尾部节点的空间并替换为新数据
func (c *cache) put(key string, val Value, expireAt int64, onEvicted func(string, Value)) int {
	if idx, ok := c.hmap[key]; ok {
		c.m[idx-1].v, c.m[idx-1].expireAt = val, expireAt
		c.adjust(idx, prev, next) // 刷新到链表头部
		return 0
	}

	// 缓存容量已满：触发 LRU 淘汰
	if c.last == uint16(cap(c.m)) {
		tail := &c.m[c.dlnk[0][prev]-1] // 获取最少使用的尾部节点
		if onEvicted != nil && (*tail).expireAt > 0 {
			onEvicted((*tail).k, (*tail).v)
		}

		delete(c.hmap, (*tail).k) // 从哈希表中移除旧键
		// 复用尾部节点的空间，写入新数据
		c.hmap[key], (*tail).k, (*tail).v, (*tail).expireAt = c.dlnk[0][prev], key, val, expireAt
		c.adjust(c.dlnk[0][prev], prev, next) // 将复用后的节点移动到链表头部

		return 1
	}

	// 缓存未满：直接在数组中分配新节点
	c.last++
	if len(c.hmap) <= 0 {
		c.dlnk[0][prev] = c.last
	} else {
		c.dlnk[c.dlnk[0][next]][prev] = c.last
	}

	// 初始化新节点并更新链表指针
	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt
	c.dlnk[c.last] = [2]uint16{0, c.dlnk[0][next]}
	c.hmap[key] = c.last
	c.dlnk[0][next] = c.last

	return 1
}

// get 从缓存中获取键对应的节点和状态，如果找到，则将其移动到链表头部(最近使用)
func (c *cache) get(key string) (*node, int) {
	if idx, ok := c.hmap[key]; ok {
		c.adjust(idx, prev, next)
		return &c.m[idx-1], 1
	}
	return nil, 0
}

// del 从缓存中伪删除键对应的项
// 为了避免真正的内存释放和数组元素移动，我们将其过期时间置为 0，
// 并将其移动到链表尾部，这样下次 put 发生淘汰时，它会被优先复用
func (c *cache) del(key string) (*node, int, int64) {
	if idx, ok := c.hmap[key]; ok && c.m[idx-1].expireAt > 0 {
		e := c.m[idx-1].expireAt
		c.m[idx-1].expireAt = 0   // 标记为已删除
		c.adjust(idx, next, prev) // 移动到链表尾部
		return &c.m[idx-1], 1, e
	}

	return nil, 0, 0
}

// 遍历缓存中的所有有效项
func (c *cache) walk(walker func(key string, value Value, expireAt int64) bool) {
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k, c.m[idx-1].v, c.m[idx-1].expireAt) {
			return
		}
	}
}

// 调整节点在链表中的位置
// 当 from=0, to=1 时，移动到链表头部；否则移动到链表尾部
func (c *cache) adjust(idx, from, to uint16) {
	if c.dlnk[idx][from] != 0 {
		c.dlnk[c.dlnk[idx][to]][from] = c.dlnk[idx][from]
		c.dlnk[c.dlnk[idx][from]][to] = c.dlnk[idx][to]
		c.dlnk[idx][from] = 0
		c.dlnk[idx][to] = c.dlnk[0][to]
		c.dlnk[c.dlnk[0][to]][from] = idx
		c.dlnk[0][to] = idx
	}
}

func (s *lru2Store) _get(key string, idx, level int32) (*node, int) {
	if n, st := s.caches[idx][level].get(key); st > 0 && n != nil {
		currentTime := Now()
		if n.expireAt <= 0 || currentTime >= n.expireAt {
			// 过期或已删除
			return nil, 0
		}
		return n, st
	}

	return nil, 0
}

func (s *lru2Store) delete(key string, idx int32) bool {
	// 从一级、二级缓存中分别删除
	n1, s1, _ := s.caches[idx][0].del(key)
	n2, s2, _ := s.caches[idx][1].del(key)
	deleted := s1 > 0 || s2 > 0

	if deleted && s.onEvicted != nil {
		if n1 != nil && n1.v != nil {
			s.onEvicted(key, n1.v)
		} else if n2 != nil && n2.v != nil {
			s.onEvicted(key, n2.v)
		}
	}

	if deleted {
		// s.expirations.Delete(key)
	}

	return deleted
}

func (s *lru2Store) cleanupLoop() {
	for range s.cleanupTick.C {
		currentTime := Now()

		for i := range s.caches {
			s.locks[i].Lock()

			// 检查并清理过期项目
			var expiredKeys []string

			s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
				if expireAt > 0 && currentTime >= expireAt {
					expiredKeys = append(expiredKeys, key)
				}
				return true
			})

			s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
				if expireAt > 0 && currentTime >= expireAt {
					for _, k := range expiredKeys {
						if key == k {
							// 避免重复
							return true
						}
					}
					expiredKeys = append(expiredKeys, key)
				}
				return true
			})

			for _, key := range expiredKeys {
				s.delete(key, int32(i))
			}

			s.locks[i].Unlock()
		}
	}
}
