package KakaCache

import (
	"context"
)

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
