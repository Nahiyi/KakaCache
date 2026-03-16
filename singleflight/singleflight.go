package singleflight

import (
	"sync"
)

// 正在进行或已结束的请求
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group 管理所有种类的请求
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能
}

// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// 先创建空call
	ongoingCall := &call{}
	// 使用 LoadOrStore 保证检查与设置的原子性
	// 如果 key 已经存在，loaded 为 true，actual 为已存在的 call
	// 如果 key 不存在，loaded 为 false，actual 为我们新创建的 ongoingCall
	actual, loaded := g.m.LoadOrStore(key, ongoingCall)

	if loaded {
		// 只有 loaded 为 true，说明之前已经有请求在处理了
		// 我们不需要新的 ongoingCall，丢弃它，等待 actual 完成
		ongoingCall = actual.(*call)
		ongoingCall.wg.Wait()
		return ongoingCall.val, ongoingCall.err
	}

	// loaded 为 false，说明本次的请求是第一个
	// 只有当前是第一个请求，才对wg执行Add(1)
	ongoingCall.wg.Add(1)
	defer ongoingCall.wg.Done() // 加在defer中，防止业务fn()报panic

	// 由本请求来执行 fn() 业务方法
	ongoingCall.val, ongoingCall.err = fn()

	// 请求完成后，清理本“key-请求”键值对
	g.m.Delete(key)

	return ongoingCall.val, ongoingCall.err
}
