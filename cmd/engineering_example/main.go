// engineering_example 演示 actor-go 的工程化用法：
// 推荐的包结构、一条真实的请求链路、持久化与共享数据的边界。
//
//	go run ./cmd/engineering_example             # 跑默认剧本，数据落在 ./data
//	go run ./cmd/engineering_example -v          # 同时打印框架日志（每条消息的收发）
//	go run ./cmd/engineering_example -data TMP   # 换一个落盘目录
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/lcy03406/actor-go/cmd/engineering_example/demo"
	"github.com/lcy03406/actor-go/cmd/engineering_example/setup"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "出错:", err)
		os.Exit(1)
	}
}

func run() error {
	dataDir := flag.String("data", "./data", "grain 落盘目录")
	verbose := flag.Bool("v", false, "打印框架日志（含每条消息的收发）")
	flag.Parse()

	level := slog.LevelWarn
	if *verbose {
		// actor 在 Info 级别逐条打印收发，教学时很有用，跑默认剧本时太吵。
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	srv, err := setup.New(setup.Config{DataDir: *dataDir, Logger: logger})
	if err != nil {
		return err
	}
	defer srv.Close() // 优雅停机：退出钩子在这里把 grain 落盘

	return demo.Run(context.Background(), srv.Mgr, os.Stdout)
}
