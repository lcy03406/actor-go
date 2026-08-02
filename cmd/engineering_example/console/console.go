// console 包提供交互式 REPL 控制台。
// 负责命令解析、格式化输出，通过 cluster 包与 Actor 通信。
package console

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lcy03406/actor-go/cluster"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/chat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/attr"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/inventory"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/skill"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
	"github.com/lcy03406/actor-go/cmd/engineering_example/setup"
)

// Run 启动交互式 REPL。
func Run(ctx context.Context, router *setup.Router, mem *setup.DynamicMembership) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])
		args := parts[1:]

		switch cmd {
		case "login":
			cmdLogin(router, args)
		case "attack":
			cmdAttack(ctx, router, args)
		case "heal":
			cmdHeal(ctx, router, args)
		case "gold":
			cmdAddGold(ctx, router, args)
		case "status":
			cmdPlayerStatus(ctx, router, args)
		case "exp":
			cmdAddExp(ctx, router, args)
		case "attrs":
			cmdQueryAttr(ctx, router, args)
		case "upgrade":
			cmdUpgradeAttr(ctx, router, args)
		case "additem":
			cmdAddItem(ctx, router, args)
		case "rmitem":
			cmdRemoveItem(ctx, router, args)
		case "bag":
			cmdListItems(ctx, router, args)
		case "use":
			cmdUseItem(ctx, router, args)
		case "learn":
			cmdLearnSkill(ctx, router, args)
		case "cast":
			cmdCastSkill(ctx, router, args)
		case "skills":
			cmdListSkills(ctx, router, args)
		case "room":
			cmdCreateRoom(router, args)
		case "join":
			cmdJoinRoom(ctx, router, args)
		case "roominfo":
			cmdRoomInfo(ctx, router, args)
		case "chat":
			cmdChat(ctx, router, args)
		case "pjoin":
			cmdPlayerJoinRoom(ctx, router, args)
		case "psend":
			cmdPlayerSendChat(ctx, router, args)
		case "info":
			printClusterInfo(router)
		case "nodes":
			printNodeStatus(router)
		case "migrate":
			cmdMigrate(mem, router, args)
		case "help":
			printHelp()
		case "quit", "exit":
			fmt.Println("再见!")
			os.Exit(0)
		default:
			fmt.Printf("未知命令: %s (输入 help 查看帮助)\n", cmd)
		}
		fmt.Print("> ")
	}
}

// ─── Player ───

func cmdLogin(router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: login <id> <hp>")
		return
	}
	hp, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](router, id, &player.Login{InitHP: hp, InitLevel: 1})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ Player %s 已创建 (HP=%d)\n", args[0], hp)
	}
}

func cmdAttack(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: attack <id> <damage>")
		return
	}
	dmg, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &player.Attack{Damage: dmg})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 受到 %d 伤害, HP=%d 存活=%v\n", args[0], dmg, reply.RemainingHP, reply.Alive)
	}
}

func cmdHeal(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: heal <id> <amount>")
		return
	}
	amt, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &player.Heal{Amount: amt})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 治疗 %d, HP=%d\n", args[0], amt, reply.NewHP)
	}
}

func cmdAddGold(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: gold <id> <amount>")
		return
	}
	amt, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &player.AddGold{Amount: amt})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 获得 %d 金币, 总计=%d\n", args[0], amt, reply.NewGold)
	}
}

func cmdPlayerStatus(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 1 {
		fmt.Println("用法: status <id>")
		return
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &player.PlayerStatusReq{})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}
	fmt.Printf("══════ %s 状态 ══════\n", args[0])
	fmt.Printf("  HP=%d  Level=%d  Gold=%d\n", reply.HP, reply.Level, reply.Gold)
	fmt.Printf("  [属性] Exp=%d  Atk=%d  Def=%d  Speed=%d\n",
		reply.Attr.Exp, reply.Attr.Atk, reply.Attr.Def, reply.Attr.Speed)
	fmt.Printf("  [背包] 容量=%d  道具数=%d\n", reply.Inventory.Capacity, len(reply.Inventory.Items))
	for _, item := range reply.Inventory.Items {
		fmt.Printf("    %dx %s (ID=%d, 类型=%s)\n", item.Count, item.Name, item.ID, item.Type)
	}
	fmt.Printf("  [技能] 槽位=%d/%d\n", len(reply.Skill.Learned), reply.Skill.MaxSlots)
	for _, s := range reply.Skill.Learned {
		fmt.Printf("    %s Lv.%d CD=%d\n", s.Name, s.Level, s.CoolDown)
	}
	fmt.Println("════════════════════════")
}

// ─── 属性 ───

func cmdAddExp(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: exp <id> <amount>")
		return
	}
	amt, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &attr.AddExp{Amount: amt})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}
	info := fmt.Sprintf("Exp=%d Level=%d", reply.Exp, reply.Level)
	if reply.LevelUp {
		info += " ★升级了!"
	}
	fmt.Printf("✓ %s 获得 %d 经验, %s\n", args[0], amt, info)
}

func cmdQueryAttr(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 1 {
		fmt.Println("用法: attrs <id>")
		return
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &attr.QueryAttr{})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 属性: Level=%d Exp=%d Atk=%d Def=%d Speed=%d\n",
			args[0], reply.Level, reply.Exp, reply.Atk, reply.Def, reply.Speed)
	}
}

func cmdUpgradeAttr(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: upgrade <id> <stat>   stat: atk, def, speed")
		return
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &attr.UpgradeAttr{Stat: args[1]})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else if reply == nil {
		fmt.Printf("✗ %s 升级失败（金币不足或属性名错误）\n", args[0])
	} else {
		fmt.Printf("✓ %s 升级 %s=%d 消耗 %d 金币\n", args[0], reply.Stat, reply.Value, reply.Cost)
	}
}

// ─── 道具 ───

func cmdAddItem(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 5 {
		fmt.Println("用法: additem <id> <itemId> <name> <count> <type>")
		fmt.Println("  type: potion, weapon, material")
		return
	}
	itemID, _ := strconv.Atoi(args[1])
	cnt, _ := strconv.Atoi(args[3])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &inventory.AddItem{ID: itemID, Name: args[2], Count: cnt, Type: args[4]})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else if !reply.Added {
		fmt.Printf("✗ %s 背包已满\n", args[0])
	} else {
		fmt.Printf("✓ %s 获得 %dx%s (总计=%d)\n", args[0], cnt, args[2], reply.TotalCount)
	}
}

func cmdRemoveItem(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 3 {
		fmt.Println("用法: rmitem <id> <itemId> <count>")
		return
	}
	itemID, _ := strconv.Atoi(args[1])
	cnt, _ := strconv.Atoi(args[2])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &inventory.RemoveItem{ID: itemID, Count: cnt})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 消耗 %dx%s (剩余=%d)\n", args[0], cnt, reply.ItemName, reply.Remaining)
	}
}

func cmdListItems(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 1 {
		fmt.Println("用法: bag <id>")
		return
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &inventory.ListItems{})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}
	fmt.Printf("✓ %s 背包 (容量=%d/%d):\n", args[0], reply.Used, reply.Capacity)
	if len(reply.Items) == 0 {
		fmt.Println("  (空)")
	}
	for _, item := range reply.Items {
		fmt.Printf("  %dx %s (ID=%d, 类型=%s)\n", item.Count, item.Name, item.ID, item.Type)
	}
}

func cmdUseItem(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: use <id> <itemId>")
		return
	}
	itemID, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &inventory.UseItem{ID: itemID})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 使用 %s → %s\n", args[0], reply.ItemName, reply.Effect)
	}
}

// ─── 技能 ───

func cmdLearnSkill(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 4 {
		fmt.Println("用法: learn <id> <skillId> <name> <cost>")
		return
	}
	skID, _ := strconv.Atoi(args[1])
	cost, _ := strconv.Atoi(args[3])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &skill.LearnSkill{ID: skID, Name: args[2], Cost: cost})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else if !reply.Learned {
		fmt.Printf("✗ %s 学习失败: %s\n", args[0], reply.Reason)
	} else {
		fmt.Printf("✓ %s 学会了 %s (消耗 %d 金币)\n", args[0], args[2], cost)
	}
}

func cmdCastSkill(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 4 {
		fmt.Println("用法: cast <id> <skillId> <target> <tx> <ty>")
		return
	}
	skID, _ := strconv.Atoi(args[1])
	tx, _ := strconv.Atoi(args[3])
	ty := 0
	if len(args) >= 5 {
		ty, _ = strconv.Atoi(args[4])
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &skill.CastSkill{
		ID: skID, Target: args[2], TargetPos: logic.Point{X: tx, Y: ty},
	})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else if !reply.Cast {
		fmt.Printf("✗ %s 释放失败: %s\n", args[0], reply.Reason)
	} else {
		critInfo := ""
		if reply.Critical {
			critInfo = " ★暴击!"
		}
		fmt.Printf("✓ %s 释放 %s → %s 造成 %d 伤害%s\n",
			args[0], reply.SkillName, args[2], reply.Damage, critInfo)
	}
}

func cmdListSkills(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 1 {
		fmt.Println("用法: skills <id>")
		return
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &skill.ListSkills{})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}
	fmt.Printf("✓ %s 技能列表 (槽位=%d/%d):\n", args[0], len(reply.Skills), reply.MaxSlots)
	if len(reply.Skills) == 0 {
		fmt.Println("  (未学习任何技能)")
	}
	for _, s := range reply.Skills {
		fmt.Printf("  %s Lv.%d CD=%d\n", s.Name, s.Level, s.CoolDown)
	}
}

// ─── Room / Chat ───

func cmdCreateRoom(router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: room <id> <max_players>")
		return
	}
	rid, _ := strconv.Atoi(args[0])
	max, _ := strconv.Atoi(args[1])
	id := room.RoomId{RoomId: rid}
	err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](router, id, &room.CreateRoom{MaxPlayers: max})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ Room %d 已创建 (最大 %d 人)\n", rid, max)
	}
}

func cmdJoinRoom(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: join <roomId> <playerId>")
		return
	}
	rid, _ := strconv.Atoi(args[0])
	id := room.RoomId{RoomId: rid}
	reply, err := cluster.Call(ctx, router, id, &room.JoinRoom{PlayerId: args[1]})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ %s 加入 Room %d, 当前人数=%d\n", args[1], rid, reply.CurrentCount)
	}
}

func cmdRoomInfo(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 1 {
		fmt.Println("用法: roominfo <id>")
		return
	}
	rid, _ := strconv.Atoi(args[0])
	id := room.RoomId{RoomId: rid}
	reply, err := cluster.Call(ctx, router, id, &room.RoomInfo{})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ Room %d 最大=%d 玩家=%v\n", rid, reply.MaxPlayers, reply.PlayerIds)
	}
}

func cmdChat(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: chat <channel> <message>")
		return
	}
	id := chat.ChatId{Channel: args[0]}
	msg := strings.Join(args[1:], " ")
	reply, err := cluster.Call(ctx, router, id, &chat.SendMessage{Text: msg})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("✓ [%s] %s → echo: %s\n", args[0], msg, reply.Echo)
	}
}

// ─── 跨 Actor 通信：Player → Room / Chat ───

func cmdPlayerJoinRoom(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 2 {
		fmt.Println("用法: pjoin <playerId> <roomId>")
		fmt.Println("  演示 Player → Room 跨 Actor 通信（Player handler 内 actor.Post → Room）")
		return
	}
	rid, _ := strconv.Atoi(args[1])
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	reply, err := cluster.Call(ctx, router, id, &player.PlayerJoinRoom{RoomId: rid})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		if reply.Success {
			fmt.Printf("✓ %s → PlayerJoinRoom(%d): %s\n", args[0], rid, reply.Reason)
		} else {
			fmt.Printf("✗ %s → PlayerJoinRoom(%d) 失败: %s\n", args[0], rid, reply.Reason)
		}
	}
}

func cmdPlayerSendChat(ctx context.Context, router *setup.Router, args []string) {
	if len(args) < 3 {
		fmt.Println("用法: psend <playerId> <channel> <message>")
		fmt.Println("  演示 Player → Chat 跨 Actor 通信（Player handler 内 actor.Call → Chat）")
		return
	}
	id := types.PlayerId{ServerId: 1, OpenId: args[0]}
	msg := strings.Join(args[2:], " ")
	reply, err := cluster.Call(ctx, router, id, &player.PlayerSendChat{Channel: args[1], Text: msg})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		if reply.Success {
			fmt.Printf("✓ %s → Chat(%s): %s (echo=%s)\n", args[0], args[1], msg, reply.Echo)
		} else {
			fmt.Printf("✗ %s → Chat(%s) 失败: %s\n", args[0], args[1], reply.Reason)
		}
	}
}

// ─── 集群 ───

func printClusterInfo(router *setup.Router) {
	members := router.Members()
	self := router.Self()
	fmt.Println("══════════════════════════════════════════════")
	fmt.Printf("  集群成员 (%d 个节点):\n", len(members))
	for _, n := range members {
		marker := " "
		if n.ID == self.ID {
			marker = "*"
		}
		groups := setup.GroupMapping.GroupsOf(n.Type)
		groupStr := "全部"
		if len(groups) > 0 && n.Type != "all-in-one" {
			groupStr = strings.Join(groups, ", ")
		}
		fmt.Printf("  %s %s  地址=%s  类型=%s  Group=%s\n", marker, n.ID, n.Addr, n.Type, groupStr)
	}
	fmt.Println("══════════════════════════════════════════════")
}

func printNodeStatus(router *setup.Router) {
	self := router.Self()
	groups := setup.GroupMapping.GroupsOf(self.Type)
	groupStr := "全部"
	if len(groups) > 0 && self.Type != "all-in-one" {
		groupStr = strings.Join(groups, ", ")
	}
	fmt.Printf("节点: %s  类型: %s  地址: %s  Group: %s  成员: %d\n",
		self.ID, self.Type, self.Addr, groupStr, len(router.Members()))
}

func cmdMigrate(mem *setup.DynamicMembership, router *setup.Router, args []string) {
	if mem == nil {
		fmt.Println("错误: 当前 Membership 不支持动态变更")
		return
	}
	if len(args) < 1 {
		fmt.Println("用法: migrate join <nodeType> <addr>  或  migrate leave <nodeId>")
		return
	}
	switch strings.ToLower(args[0]) {
	case "join":
		if len(args) < 3 {
			fmt.Println("用法: migrate join <nodeType> <addr>")
			return
		}
		nodeType, addr := args[1], args[2]
		nodeID := fmt.Sprintf("%s-%s", nodeType, addr)
		mem.AddNode(cluster.Node{ID: nodeID, Addr: addr, Type: nodeType})
		fmt.Printf(">>> 节点加入: %s, 观察日志中的 [迁移] 信息\n", nodeID)
		time.Sleep(300 * time.Millisecond)
	case "leave":
		if len(args) < 2 {
			fmt.Println("用法: migrate leave <nodeId>")
			return
		}
		if args[1] == router.Self().ID {
			fmt.Println("错误: 不能移除当前节点自身")
			return
		}
		mem.RemoveNode(args[1])
		fmt.Printf(">>> 节点离开: %s, 观察日志中的 [迁移] 信息\n", args[1])
		time.Sleep(300 * time.Millisecond)
	default:
		fmt.Println("未知子命令, 可用: join, leave")
	}
}

// ─── 帮助 ───

func printHelp() {
	fmt.Println("═══════ Player 命令 ═══════")
	fmt.Println("  login <id> <hp>        创建玩家")
	fmt.Println("  attack <id> <dmg>      攻击玩家")
	fmt.Println("  heal <id> <amt>        治疗玩家")
	fmt.Println("  gold <id> <amt>        增加金币")
	fmt.Println("  status <id>            查看完整状态")
	fmt.Println("─────── 属性子模块 ───────")
	fmt.Println("  exp <id> <amt>         增加经验")
	fmt.Println("  attrs <id>             查看属性")
	fmt.Println("  upgrade <id> <stat>    升级属性 (atk/def/speed)")
	fmt.Println("─────── 道具子模块 ───────")
	fmt.Println("  additem <id> <n> <name> <cnt> <type>  添加道具")
	fmt.Println("  rmitem <id> <itemId> <cnt>            移除道具")
	fmt.Println("  bag <id>                              查看背包")
	fmt.Println("  use <id> <itemId>                     使用道具")
	fmt.Println("─────── 技能子模块 ───────")
	fmt.Println("  learn <id> <skId> <name> <cost>      学习技能")
	fmt.Println("  cast <id> <skId> <target> <tx> <ty>  释放技能 (含坐标距离判定)")
	fmt.Println("  skills <id>                           技能列表")
	fmt.Println("═══════ 其他命令 ═══════")
	fmt.Println("  room <id> <max>        创建房间")
	fmt.Println("  join <rid> <pid>       加入房间")
	fmt.Println("  roominfo <id>          查询房间")
	fmt.Println("  chat <ch> <msg>        发送消息")
	fmt.Println("──── 跨Actor通信演示 ────")
	fmt.Println("  pjoin <pid> <rid>      Player→Room (actor.Post)")
	fmt.Println("  psend <pid> <ch> <msg> Player→Chat (actor.Call)")
	fmt.Println("  info                   集群拓扑")
	fmt.Println("  nodes                  节点状态")
	fmt.Println("  migrate join/leave ... 模拟迁移")
	fmt.Println("  quit                   退出")
}
