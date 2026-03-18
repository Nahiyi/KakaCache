package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"log"
	"net/http"
	"time"

	lcache "github.com/Nahiyi/KakaCache"
)

// 这个文件用于进阶测试：提供 HTTP 接口，允许外部精准控制向哪个节点请求哪个 Key
func main() {
	// logrus.SetLevel(logrus.DebugLevel)

	port := flag.Int("port", 8001, "节点端口")
	apiPort := flag.Int("api", 9001, "HTTP API 端口 (用于外部查询)")
	nodeID := flag.String("node", "A", "节点标识符")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)
	logrus.Infof("[节点%s] gRPC 监听地址: %s, HTTP API 监听地址: :%d", *nodeID, addr, *apiPort)

	// 创建 Server
	// 注意：这里本机 etcd 地址配置为 localhost:2379，测试改为实际的
	node, err := lcache.NewServer(addr, "kaka-cache",
		lcache.WithEtcdEndpoints([]string{"localhost:2379"}),
		lcache.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		logrus.Fatalf("创建节点失败: %v", err)
	}

	// 创建 ClientPicker
	picker, err := lcache.NewClientPicker(addr)
	if err != nil {
		logrus.Fatalf("创建节点选择器失败: %v", err)
	}

	// 创建 Group，修改回源逻辑，让它明确打印是哪个节点在真正执行回源！
	group := lcache.NewGroup("test", 2<<20, lcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			logrus.Warnf("[回源] 节点 %s 正在查询数据库，Key: [%s]", *nodeID, key)
			return []byte(fmt.Sprintf("DB值_来自节点_%s_Key_%s", *nodeID, key)), nil
		}),
	)
	group.RegisterPeers(picker)

	// 启动 gRPC 服务
	go func() {
		if err := node.Start(); err != nil {
			logrus.Fatalf("启动节点失败: %v", err)
		}
	}()

	// 等待集群稳定
	logrus.Infof("[节点%s] 正在等待服务注册与发现...", *nodeID)
	time.Sleep(5 * time.Second)

	// 预先向集群写入一些数据（每个节点都尝试写入）
	logrus.Infof("[节点%s] HTTP API 已就绪 (无预热数据，将触发回源)...", *nodeID)

	// 启动 HTTP 服务，用于接收外部查询请求（即模拟三方客户端）
	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		// 记录谁接收了外部请求
		logrus.Infof("\n--- [HTTP 请求] 外部用户 -> 节点(%s), 查询 Key: %s ---", *nodeID, key)

		// 打印一致性哈希路由预测
		if _, ok, isSelf := picker.PickPeer(key); ok {
			if isSelf {
				logrus.Debugf("[路由分析] Key [%s] 根据一致性哈希，应该由 [本节点 %s] 处理", key, *nodeID)
			} else {
				// 由于 Peer 接口不直接暴露地址，这里我们只能知道是远程节点
				logrus.Debugf("[路由分析] Key [%s] 根据一致性哈希，应该由 [远程节点] 处理 (将发起 RPC)", key)
			}
		} else {
			logrus.Debugf("[路由分析] 尚未发现任何节点，将由本节点处理")
		}

		start := time.Now()
		view, err := group.Get(context.Background(), key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		logrus.Infof("--- [HTTP 响应] 耗时: %v ---\n", time.Since(start))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(view.ByteSLice()) // 注意：这里调用的是ByteSLice()返回切片
	})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *apiPort), nil))
}
