// 工程化示例：多 Actor 子包 + 子模块 + grain 持久化 + logic 公共库。
//
// 启动: go run . -type all-in-one -addr localhost:8001
//
// 项目结构详见 README.md。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lcy03406/actor-go/cmd/engineering_example/console"
	"github.com/lcy03406/actor-go/cmd/engineering_example/setup"
)

func main() {
	nodeType := flag.String("type", "all-in-one", "节点类型")
	addr := flag.String("addr", "localhost:8001", "监听地址")
	seeds := flag.String("seeds", "", "种子节点地址，逗号分隔")
	flag.Parse()

	if _, ok := setup.GroupMapping[*nodeType]; !ok {
		log.Fatalf("未知节点类型: %s", *nodeType)
	}

	nodeID := fmt.Sprintf("%s-%s", *nodeType, *addr)
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router, mem, err := setup.StartNode(ctx, setup.NodeConfig{
		NodeType: *nodeType,
		NodeID:   nodeID,
		Addr:     *addr,
		Seeds:    *seeds,
	})
	if err != nil {
		log.Fatalf("启动失败: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	console.PrintBanner(*nodeType, *addr, router)
	go console.Run(ctx, router, mem)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("正在关闭...")
	router.Close()
	log.Println("已退出")
}
