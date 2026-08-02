# engineering_example — Actor-Go 工程化示例

展示在真实项目中如何使用 actor-go 组织代码：**多 Actor 子包 + 子模块二级子包 + grain 持久化 + 公共逻辑库**。

## 项目结构

```
cmd/engineering_example/
├── main.go                          # 入口（仅启动 + 信号处理）
│
├── console/                         # 交互式 REPL 控制台
│   ├── console.go                      命令解析 + 格式化输出
│   └── banner.go                       启动横幅
│
├── logic/                           # 公共游戏逻辑（零框架依赖）
│   ├── damage.go                       伤害公式、治疗公式、经验公式
│   └── pathfinding.go                  A* 寻路、距离判定
│
├── actor/
│   ├── player/                      # Player Actor（聚合根）
│   │   ├── types/                      共享类型（只依赖 actor + grain）
│   │   │   ├── types.go                    PlayerId + PlayerState
│   │   │   ├── attr.go                     AttrState
│   │   │   ├── inventory.go                Item + InventoryState
│   │   │   ├── skill.go                    Skill + SkillState
│   │   │   └── snapshot.go                 GrainState + Snapshotter
│   │   │
│   │   ├── attr/                      属性子模块（只依赖 types/ + logic/）
│   │   │   ├── add_exp.go                  AddExp 请求 + Handle
│   │   │   ├── query.go                    QueryAttr
│   │   │   └── upgrade.go                  UpgradeAttr
│   │   │
│   │   ├── inventory/                道具子模块（只依赖 types/）
│   │   │   ├── add.go                      AddItem
│   │   │   ├── remove.go                   RemoveItem
│   │   │   ├── list.go                     ListItems
│   │   │   └── use.go                      UseItem（跨模块：回复HP/加Atk）
│   │   │
│   │   ├── skill/                    技能子模块（只依赖 types/ + logic/）
│   │   │   ├── learn.go                    LearnSkill
│   │   │   ├── cast.go                     CastSkill（引用 logic.CalcDamage）
│   │   │   └── list.go                     ListSkills
│   │   │
│   │   ├── handler.go                     handler+RPC注册（grain.WrapSpawnHandler2）
│   │   ├── login.go                       登录（spawn handler）
│   │   ├── attack.go                      攻击（引用 logic.CalcDamage）
│   │   ├── heal.go / addgold.go / close.go
│   │   ├── status.go                      完整状态查询
│   │   └── check_ownership.go             集群迁移检查
│   │
│   ├── room/                         # Room Actor
│   └── chat/                         # Chat Actor
│
└── setup/                            # 组装层（只依赖 player/ 一层）
    ├── setup.go                          PersistenceManager + Group + RPC + 集群
    └── membership.go                     动态成员管理
```

## 依赖方向

```
types/    ──→ actor + grain                    ← 零业务依赖
logic/    ──→ 标准库                             ← 零框架依赖
attr/     ──→ types/ + logic/                  ← 子模块
inventory/──→ types/
skill/    ──→ types/ + logic/
player/   ──→ types/ + attr/ + inventory/ + skill/   ← 聚合根
setup/    ──→ player/ + room/ + chat/           ← 只依赖一层
console/  ──→ player/ + setup/ + cluster/       ← 交互层
main.go   ──→ setup/ + console/
```

**无循环依赖**。子模块之间互不依赖，通过操作 `PlayerState` 的不同字段实现交互。

## 核心设计

### 1. RequestHandler 模式（请求与处理合一）

每个请求文件包含：结构体 + Reply + `ReqType` + `Handle`。

```go
// attr/add_exp.go
type AddExp struct{ Amount int `json:"amount"` }
type AddExpReply struct{ Exp, Level int; LevelUp bool }

func (*AddExp) ReqType(_ types.PlayerId, _ *AddExpReply) string { return "AddExp" }

func (req *AddExp) Handle(ctx *types.PlayerActorCtx, spawning bool) (*AddExpReply, error) {
    data := &ctx.State().Data
    data.Attr.Exp += req.Amount
    // 使用 logic 包计算升级
    for data.Attr.Exp >= logic.CalcExpToLevel(data.Level) { ... }
    ctx.State().Persist(ctx)  // 持久化
    return &AddExpReply{...}, nil
}
```

注册极简，handler.go 中一行一个：

```go
actor.RegisterQueryHandler2(b, (*attr.AddExp)(nil))
```

### 2. 依赖翻转（player 作为聚合根）

子模块不依赖 player 包，player 包主动导入所有子模块进行注册：

- 子模块 `attr/` 只依赖 `types/` + `logic/`
- `player/handler.go` import `attr/` + `inventory/` + `skill/`，集中注册
- `setup/` 只调用 `player.RegisterHandlers()` + `player.RegisterRPC()`，不知道子模块存在

### 3. grain 持久化

`player/types/snapshot.go` 定义 `PlayerSnapshotter = ShotSelf[PlayerState]`，完整保存 PlayerState。

```go
// handler.go — spawn 包装
actor.RegisterSpawn(b, grain.WrapSpawnHandler2(pm, (*Login)(nil)))

// 业务 handler 中
ctx.State().Persist(ctx)     // 保存 + 续租
ctx.State().Deactivate(ctx)  // 保存 + 释放租约 + Quit
```

### 4. logic/ 公共库

与 actor 框架完全解耦，只依赖标准库：

| 文件 | 功能 | 引用方 |
|------|------|--------|
| `damage.go` | `CalcDamage`, `CalcHeal`, `CalcExpToLevel` | `skill/cast.go`, `player/attack.go`, `attr/add_exp.go` |
| `pathfinding.go` | `FindPath`(A*), `InRange`, `ManhattanDist` | `skill/cast.go` |

可独立单元测试，不依赖任何 actor 状态。

## 启动

```bash
# 单节点（承载所有 Actor）
go run . -type all-in-one -addr localhost:8001

# 多节点异构集群
go run . -type player-server -addr localhost:8001
go run . -type room-server   -addr localhost:8002 -seeds localhost:8001
go run . -type chat-server   -addr localhost:8003 -seeds localhost:8001
```

## 交互命令

```
=== Player ===
login <id> <hp>                      创建玩家
attack <id> <dmg>                    攻击
heal <id> <amt>                      治疗
gold <id> <amt>                      增加金币
status <id>                          查看完整状态（含属性/背包/技能）

=== 属性子模块 ===
exp <id> <amt>                       增加经验
attrs <id>                           查看属性
upgrade <id> <stat>                  升级属性 (atk/def/speed)

=== 道具子模块 ===
additem <id> <itemId> <name> <cnt> <type>   添加道具 (potion/weapon/material)
rmitem <id> <itemId> <cnt>                  移除道具
bag <id>                                    查看背包
use <id> <itemId>                           使用道具

=== 技能子模块 ===
learn <id> <skillId> <name> <cost>          学习技能
cast <id> <skillId> <target> <tx> <ty>      释放技能（含坐标距离判定）
skills <id>                                 技能列表

=== 其他 ===
room <id> <max>        创建房间
join <rid> <pid>       加入房间
roominfo <id>          查询房间
chat <ch> <msg>        发送消息
info / nodes / migrate 集群管理
```

## 新增子模块的步骤

1. 在 `player/types/` 添加子模块数据结构（一个文件）
2. 在 `player/` 下新建子模块目录，每个请求一个文件（结构体+Reply+ReqType+Handle）
3. 在 `player/handler.go` 中 import 子模块 + 一行 `RegisterQueryHandler2`
4. 在 `player/handler.go` 的 `RegisterRPC` 中加一行 `rpc.RegisterRequest`
5. 在 `console/console.go` 中添加 REPL 命令处理

无需修改 `setup/` 和 `main.go`。
