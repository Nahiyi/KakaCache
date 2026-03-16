package store

import (
	"container/list"
	"sync"
	"time"
)

// lruCache 是基于标准库 list 的 LRU 缓存实现
type lruCache struct {
	mu              sync.RWMutex
	list            *list.List                    // 双向链表，用于维护 LRU 顺序
	items           map[string]*list.Element      // 键到链表节点的映射
	expires         map[string]time.Time          // 键到过期时间的映射
	maxBytes        int64                         // 所有K-V一共最大允许占用的字节数
	usedBytes       int64                         // 当前一共占用的字节数
	onEvicted       func(key string, value Value) // 缓存因为满了、过期了被删除时触发的删除回调
	cleanupInterval time.Duration                 // 清理间隔
	cleanupTicker   *time.Ticker                  // 定时器，定时检查过期数据并清理（定时删除，内存友好）
	closeCh         chan struct{}                 // 用于优雅关闭清理协程
}

// lruEntry 表示缓存中的一个条目（K-V格式）
type lruEntry struct {
	key   string
	value Value
}

// newLRUCache 根据自定义配置项，创建一个新的 LRU 缓存实例
func newLRUCache(opts Options) *lruCache {
	// 设置默认清理间隔
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		// 默认是一分钟
		cleanupInterval = time.Minute
	}

	cache := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	// time.NewTicker(d)：每隔 d 时间向 ticker.C 管道发一次当前时间
	cache.cleanupTicker = time.NewTicker(cache.cleanupInterval)
	// 启动定期清理协程
	go cache.cleanupLoop()

	return cache
}

// Get 获取缓存项，如果存在且未过期则返回
func (c *lruCache) Get(key string) (Value, bool) {
	// 获取读锁，读读共享，读写互斥
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}

	// 检查是否过期
	if expTime, hasExp := c.expires[key]; hasExp && time.Now().After(expTime) {
		c.mu.RUnlock()

		// 异步删除过期项，避免在读锁内操作（此处：惰性删除策略）
		go c.Delete(key)

		return nil, false
	}

	// 获取值并释放读锁
	entry := elem.Value.(*lruEntry)
	cachedValue := entry.value
	c.mu.RUnlock()

	// 更新该缓存节点为最新，需要加写锁
	c.mu.Lock()
	// 再次检查元素是否仍然存在，且是否仍然是同一个节点（可能在获取写锁期间被其他协程删除并重新添加了新的节点）
	if curElem, ok := c.items[key]; ok && curElem == elem {
		c.list.MoveToBack(elem) // 移动到队尾，默认最新
	}
	c.mu.Unlock()

	return cachedValue, true
}

// Set 添加或更新缓存项
func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration 添加或更新缓存项，并设置过期时间；expiration：相对时间值，即ttl
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 计算过期时间
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime // 更新过期时间映射
	} else {
		delete(c.expires, key) // 删除过期时间映射，ttl<=0，即不过期
	}

	// 如果键已存在，更新值
	if elem, ok := c.items[key]; ok {
		oldEntry := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - oldEntry.value.Len()) // 更新已使用空间
		oldEntry.value = value                                   // 更新缓存value
		c.list.MoveToBack(elem)                                  // 将当前缓存节点移动到最新访问
		return nil
	}

	// 键不存在，即添加新项
	entry := &lruEntry{key: key, value: value}
	elem := c.list.PushBack(entry)               // 添加到末尾，表示最新
	c.items[key] = elem                          // 哈希表中也添加
	c.usedBytes += int64(len(key) + value.Len()) // 更新已用空间

	// 检查是否需要淘汰旧项
	c.evict()

	return nil
}

// Delete 从缓存中删除指定键的项
func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// Clear 清空缓存
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果设置了回调函数，遍历所有项调用回调
	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

// Len 返回缓存中的项数
func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

// removeElement 从缓存中删除元素，调用此方法前必须持有锁
func (c *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	c.list.Remove(elem)                                      // 从双向链表中删除
	delete(c.items, entry.key)                               // 再从哈希表中删除
	delete(c.expires, entry.key)                             // 同时删除额外的过期时间哈希表
	c.usedBytes -= int64(len(entry.key) + entry.value.Len()) // 更新已用空间

	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

// evict 清理过期和超出内存限制的缓存，调用此方法前必须持有锁
func (c *lruCache) evict() {
	// 1. 主动清理过期数据（遍历"过期时间哈希表"以清理）
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			// 过期且存在，则删除
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}

	// 2. 如果内存满了，清理最久未使用的（LRU算法核心）
	// 只要当前用量 > 最大允许量，就一直循环删
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front() // 获取最久未使用的项（即链表头部）
		if elem != nil {
			c.removeElement(elem)
		}
	}
}

// cleanupLoop 定期清理过期缓存的协程
func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Close 关闭缓存，停止清理协程
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop() // 关闭计时器，防止内存泄漏
		close(c.closeCh)
	}
}

// GetWithExpiration 获取缓存项及其剩余过期时间
func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, 0, false
	}

	// 检查是否过期
	now := time.Now()
	if expTime, hasExp := c.expires[key]; hasExp {
		if now.After(expTime) {
			// 已过期
			return nil, 0, false
		}

		// 计算剩余过期时间
		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, ttl, true
	}

	// 无过期时间
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}

// GetExpiration 获取键的过期时间
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime, ok := c.expires[key]
	return expTime, ok
}

// UpdateExpiration 更新过期时间；与SetWithExpiration方法相似的逻辑
func (c *lruCache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; !ok {
		return false
	}

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	return true
}

// UsedBytes 返回当前使用的字节数
func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock() // 读操作，只加读锁即可
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes 返回最大允许字节数
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes 设置最大允许字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
