package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	lcache "github.com/Nahiyi/KakaCache"
)

// 集成测试入口该程序可以启动一个KakaCache节点，并演示服务注册、发现和缓存交互
func main() {
	// 解析命令行参数
	// port: 节点监听的端口 (例如 8001, 8002, 8003)
	// node: 节点的名称标识 (例如 A, B, C)
	port := flag.Int("port", 8001, "节点端口")
	nodeID := flag.String("node", "A", "节点标识符")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[节点%s] 正在启动，监听地址: %s", *nodeID, addr)

	// 创建gRPC服务端
	// 负责监听端口，处理来自其他节点的 gRPC 请求，并向 Etcd 注册自己
	node, err := lcache.NewServer(addr, "kama-cache",
		lcache.WithEtcdEndpoints([]string{"localhost:2379"}),
		lcache.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal("创建节点失败:", err)
	}

	// 创建 ClientPicker (客户端选择器/路由)
	// 负责连接Etcd，感知其他节点的存在，并维护一致性哈希环
	// 当需要Get/Set数据时，会问它："这个key归谁管？"
	picker, err := lcache.NewClientPicker(addr)
	if err != nil {
		log.Fatal("创建节点选择器失败:", err)
	}

	// 创建test缓存组
	// 这是用户直接交互的接口。定义了缓存的大小、名称以及当缓存未命中时如何回源加载数据
	group := lcache.NewGroup("test", 2<<20, lcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			// 模拟慢速数据库查询
			log.Printf("[节点%s] 缓存未命中，触发回源加载: key=%s", *nodeID, key)
			return []byte(fmt.Sprintf("这是节点%s从数据库加载的值: %s", *nodeID, key)), nil
		}),
	)

	// 将 Picker 注册到 Group
	// 这样 Group 才知道当本地没有数据时，该问谁要
	group.RegisterPeers(picker)

	// 启动 Server (非阻塞)
	go func() {
		log.Printf("[节点%s] gRPC 服务启动中...", *nodeID)
		if err := node.Start(); err != nil {
			log.Fatal("启动节点失败:", err)
		}
	}()

	// 预热等待
	// 给一点时间让节点完成注册，让 Picker 完成服务发现
	log.Printf("[节点%s] 等待服务注册与发现...", *nodeID)
	time.Sleep(5 * time.Second)

	ctx := context.Background()

	// 场景演示1: 设置本地数据
	// 每个节点启动时，先往集群里塞一个自己特有的 key
	localKey := fmt.Sprintf("key_%s", *nodeID)
	localValue := []byte(fmt.Sprintf("Value from %s", *nodeID))

	fmt.Printf("\n=== [节点%s] 演示 1: 设置数据 ===\n", *nodeID)
	// 这里的 Set 可能会被路由到其他节点，取决于一致性哈希的结果
	err = group.Set(ctx, localKey, localValue)
	if err != nil {
		log.Fatal("设置数据失败:", err)
	}
	fmt.Printf("成功设置键值对: %s -> %s\n", localKey, localValue)

	// 再次等待，确保所有节点都上线并同步了元数据
	// 测试为手动分别启动三个终端
	log.Printf("[节点%s] 等待集群稳定 (30秒)... 现在可以启动其他节点了", *nodeID)
	time.Sleep(30 * time.Second)

	// 打印当前视野中的所有节点，看看 Etcd 发现是否工作正常
	picker.PrintPeers()

	// ------------------------------------

	// 场景演示2: 获取数据 (本地 + 远程)
	fmt.Printf("\n=== [节点%s] 演示 2: 获取数据 ===\n", *nodeID)

	// 定义一组测试 Key，理论上它们会分布在不同的节点上
	testKeys := []string{"key_A", "key_B", "key_C"}

	for _, key := range testKeys {
		fmt.Printf("\n--- 尝试查询 Key: %s ---\n", key)

		// 记录开始时间
		start := time.Now()

		// 发起查询
		val, err := group.Get(ctx, key)

		if err == nil {
			fmt.Printf("结果: 命中! 值: [%s] (耗时: %v)\n", val.String(), time.Since(start))
		} else {
			fmt.Printf("结果: 失败. 错误: %v\n", err)
		}
	}

	// 打印统计信息
	// 看看有多少次是本地命中，多少次是远程获取
	stats := group.Stats()
	fmt.Printf("\n=== [节点%s] 缓存统计 ===\n%+v\n", *nodeID, stats)

	// 保持主程序运行，以便其他节点可以连接它
	log.Printf("[节点%s] 测试结束，保持服务运行中... (按 Ctrl+C 退出)", *nodeID)
	select {}
}
