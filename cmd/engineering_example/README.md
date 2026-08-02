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
│   ├── combat/                      # 跨 Actor 战斗事件协议（类型名以来源首词标注）
│   │   ├── damage.go                   PlayerDamage（落到本Player的受击事件）
│   │   └── result.go                   PlayerCombatResult（被攻击方回传攻击方的结果）
│   │
│   ├── player/                      # Player Actor（聚合根）
│   │   ├── types/                      共享类型 + 状态领域方法（只依赖 actor + grain + logic）
│   │   │   ├── types.go                    PlayerId + PlayerState + TakeDamage/ApplyHeal
│   │   │   ├── attr.go                     AttrState
│   │   │   ├── methods.go                  AttrState/PlayerState 领域方法(AddGold/Upgrade/AddExp)
│   │   │   ├── inventory.go                Item + InventoryState
│   │   │   ├── skill.go                    Skill + SkillState
│   │   │   └── snapshot.go                 GrainState + Snapshotter
│   │   │
│   │   ├── attr/                      属性子模块（只依赖 types/ + logic/）
│   │   │   ├── add_exp.go                  AddExp 请求入口（调 PlayerState.AddExp）
│   │   │   ├── query.go                    QueryAttr
│   │   │   └── upgrade.go                  UpgradeAttr 请求入口（调 AttrState.Upgrade）
│   │   │
│   │   ├── inventory/                道具子模块（只依赖 types/）
│   │   │   ├── add.go                      AddItem
│   │   │   ├── remove.go                   RemoveItem
│   │   │   ├── list.go                     ListItems
│   │   │   └── use.go                      UseItem
│   │   │
│   │   ├── skill/                    技能子模块（只依赖 types/ + logic/ + combat/）
│   │   │   ├── learn.go                    ControlLearn（学习技能，玩家意图）
│   │   │   ├── cast.go                     ControlCast（释放技能，post PlayerDamage）
│   │   │   └── list.go                     ListSkills
│   │   │
│   │   ├── handler.go                     handler+RPC注册（spawn 时显式 Activate）
│   │   ├── login.go                       登录（spawn handler）
│   │   ├── attack.go                      ControlAttack（玩家意图，post PlayerDamage）
│   │   ├── heal.go / addgold.go / close.go
│   │   ├── joinroom.go / leaveroom.go     ControlJoinRoom / ControlLeaveRoom
│   │   ├── sendchat.go                    房间内聊天（Player→Room）
│   │   ├── status.go                      完整状态查询
│   │   └── check_ownership.go             集群迁移检查
│   │
│   ├── room/                         # Room Actor（含房间内聊天 / 战斗逻辑）
│
└── setup/                            # 组装层（只依赖 player/ 一层）
    ├── setup.go                          PersistenceManager + Group + RPC + 集群
    └── membership.go                     动态成员管理
```

## 命名约定：首词即来源

actor 世界里调用方可能来自本节点或集群另一节点，**不存在稳定的"内外"边界**，
因此用**类型名首词标注来源**，包位置不再承载语义：

| 首词 | 含义 | 示例 |
|------|------|------|
| `Control*` | 玩家/客户端主动发出的意图（入口） | `ControlAttack` `ControlHeal` `ControlCast` `ControlLearn` `ControlJoinRoom` `ControlLeaveRoom` |
| `Player*` | 落到某个 Player actor 身上的事件（由另一 actor 推来） | `PlayerDamage`（被攻击方受击） `PlayerCombatResult`（攻击方收回报） |
| `Room*` | Room actor 主动推给成员的通知 | `RoomChat` `RecordBattle` `ReceiveRoomEvent` |

## 依赖方向

```
types/    ──→ actor + grain + logic          ← 状态字段 + 状态领域方法
logic/    ──→ 标准库                             ← 零框架依赖
attr/     ──→ types/ + logic/                  ← 子模块（请求入口）
inventory/──→ types/
skill/    ──→ types/ + logic/ + combat/        ← 子模块（请求入口）
combat/   ──→ types/ + room/ + notify/         ← 跨 actor 战斗事件协议
player/   ──→ types/ + attr/ + inventory/ + skill/ + combat/ + notify/  ← 聚合根入口
room/     ──→ types/ + notify/
setup/    ──→ player/ + room/                 ← 只依赖一层
console/  ──→ player/ + setup/ + cluster/       ← 交互层
main.go   ──→ setup/ + console/
```

**无循环依赖**。状态逻辑挂在 `types` 包的状态类型上（`state 带逻辑`），handler 只做入口；
`combat` 包只承载真正跨 actor 的事件协议（`PlayerDamage`/`PlayerCombatResult`），
被 `player`/`skill` 共用，避免循环依赖。

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
// handler.go — spawn 时显式激活
actor.RegisterSpawn(b, func(ctx *PlayerActorCtx, req *Login, spawning bool) (actor.OkReply, error) {
    res, err := ctx.State().Activate(ctx, pm)
    if err != nil {
        return actor.OK, err
    }
    return req.Handle(ctx, spawning, res)
})

// 业务 handler 中
ctx.State().Persist(ctx)     // 保存 + 续租
ctx.State().Deactivate(ctx)  // 保存 + 释放租约 + Quit
```

### 4. logic/ 公共库

与 actor 框架完全解耦，只依赖标准库：

| 文件 | 功能 | 引用方 |
|------|------|--------|
| `damage.go` | `CalcDamage`, `CalcHeal`, `CalcExpToLevel` | `skill/cast.go`（技能伤害）, `types.PlayerState.TakeDamage`（受击伤害） |
| `pathfinding.go` | `FindPath`(A*), `InRange`, `ManhattanDist` | `skill/cast.go` |

可独立单元测试，不依赖任何 actor 状态。

### 5. 模块联动：成长闭环

子模块不是各自为政的演示，而是被一条**战斗成长闭环**串起来：

```
ControlAttack / ControlCast ──post──▶ PlayerDamage（落到被攻击方：同房间校验 + 扣血 + Room广播）
   │                                        │
   │ (被攻击方)                              └─ 扣血后 post PlayerCombatResult 回攻击方
   │                                               │
   │ (攻击方收到回传后，才结算奖励)                   │
   └──────────────────────────────────────────────┘
               攻击方获得 金币 + 经验（AttrState.AddGold / PlayerState.AddExp）
                                  │
                  AddExp 满则升级 ─┤ 提升 Atr.Atk/Def + MaxHP + 回满血
                                  │
           属性越强 ─▶ 下次 ControlAttack/ControlCast 伤害越高（CalcDamage 含 Atk）
                                  │
           掉血后续航 ─▶ ControlHeal（消耗金币，受 MaxHP 上限）或 use potion（受 MaxHP 上限）
                                  │
           金币出口 ─▶ heal / upgrade / learn；金币来源 ─▶ 战斗回报 / addgold
```

关键结合点：

| 模块 A | 模块 B | 结合方式 |
|--------|--------|----------|
| `ControlAttack` / `ControlCast`（伤害意图） | `Attr`（属性） | 伤害含 Atk，升级提升 Atk 直接放大输出 |
| `PlayerCombatResult`（回传） | `AttrState.AddGold` / `PlayerState.AddExp`（资源） | 攻击方**收到对面状态通知后**才结算金币+经验 |
| `PlayerState.AddExp`（升级） | `HP` / `MaxHP`（生命） | 升级提升 MaxHP 并回满血 |
| `ControlHeal` / `use potion`（续航） | `Gold` / `MaxHP` | 治疗消耗金币且受 MaxHP 上限约束 |
| `ControlAttack`（普通攻击） | `ControlCast`（技能攻击） | 二者共用 `combat.PlayerDamage` 受击链路，逻辑只有一份 |
| `combat.PlayerDamage` | `room.RecordBattle` | 受击后 post Room 广播战斗记录给同房间成员 |

这样各模块从「孤立的请求-处理」变成「互相喂养的状态机」：**攻击产生回传 → 回传结算资源 → 资源带来成长 → 成长提升战力 → 战力又放大攻击**。奖励严格发生在「收到对面状态通知之后」，跨 actor 与 component 内状态变更的边界清晰。

## 启动

```bash
# 单节点（承载所有 Actor）
go run . -type all-in-one -addr localhost:8001

# 多节点异构集群
go run . -type player-server -addr localhost:8001
go run . -type room-server   -addr localhost:8002 -seeds localhost:8001
```

## 交互命令

```
=== Player ===
login <id> <hp>                      创建玩家
heal <id> <amt>                      治疗
gold <id> <amt>                      增加金币
status <id>                          查看完整状态（含属性/背包/技能/当前房间）

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
roominfo <id>          查询房间(成员/聊天/战斗)

=== 跨Actor通信（Player 进入房间后）===
pjoin <pid> <rid>      Player 加入房间（记录当前房间）
chat <pid> <msg>       玩家在房间内聊天 Player→Room→同房间广播
attack <atk> <tgt>     玩家房间内攻击 Player→(post)Player→Room广播
leave <pid>            玩家离开房间 Player→Room
info / nodes / migrate 集群管理
```

## 新增子模块的步骤

1. 在 `player/types/` 添加子模块数据结构（一个文件）
2. 在 `player/` 下新建子模块目录，每个请求一个文件（结构体+Reply+ReqType+Handle）
3. 在 `player/handler.go` 中 import 子模块 + 一行 `RegisterQueryHandler2`
4. 在 `player/handler.go` 的 `RegisterRPC` 中加一行 `rpc.RegisterRequest`
5. 在 `console/console.go` 中添加 REPL 命令处理

无需修改 `setup/` 和 `main.go`。
