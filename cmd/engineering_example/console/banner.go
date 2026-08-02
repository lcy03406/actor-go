package console

import (
	"fmt"
	"strings"

	"github.com/lcy03406/actor-go/cmd/engineering_example/setup"
)

// PrintBanner 打印启动横幅。
func PrintBanner(nodeType, addr string, router *setup.Router) {
	groups := setup.GroupMapping[nodeType]
	groupNames := "所有"
	if len(groups) > 0 && nodeType != "all-in-one" {
		groupNames = strings.Join(groups, ", ")
	}

	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║     Actor-Go 工程化示例 — 依赖翻转 + 子模块 + 持久化       ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  节点 ID:     %-46s ║\n", router.Self().ID)
	fmt.Printf("║  节点类型:    %-46s ║\n", nodeType)
	fmt.Printf("║  监听地址:    %-46s ║\n", addr)
	fmt.Printf("║  承载 Group:  %-46s ║\n", groupNames)
	fmt.Printf("║  集群成员:    %-46d ║\n", len(router.Members()))
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Println("║  输入 help 查看所有命令，quit 退出                          ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()
}
