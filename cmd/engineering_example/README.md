# engineering_example — 工程化包结构示例

一个**能跑通的小服务端**：4 个 Actor、一条完整请求链路、grain 持久化、shared 共享配置、端到端测试。
它演示的是"真实项目里怎么摆包、怎么定消息方向、怎么划持久化边界"，不是框架 API 大全。

| 想学什么 | 去哪看 |
|---|---|
| 单个 API 怎么用（发送、注册、租约、key 定义） | `../actor_example`、`../grain_example`、`../shared_example` |
| 多节点、传输层、迁移 | `../rpc_example`、`../cluster_example` |
| **这些能力在业务代码里怎么组合** | 本目录 |

本示例只用 `actor` + `grain` + `shared` 三个包。传输层不在示例范围内 —— 真实项目里它是
WebSocket 收到的一帧 JSON，与"包怎么组织"是两件独立的事。

## 跑起来

```bash
go run ./cmd/engineering_example            # 跑一段剧本化的完整对话，数据落在 ./data
go run ./cmd/engineering_example -v         # 附带框架日志（每条消息的收发，教学时很有用）
go run ./cmd/engineering_example -data TMP  # 换一个落盘目录
go test ./cmd/engineering_example/...       # 端到端用例 + 领域纯函数单测
```

输出节选（`-v` 关掉时只打印业务对话）：

```
=== 2. 进 3 号房间：房间把成员进出广播给全体成员 ===
  alice 状态[join] Lv1 HP 100/100 金币 0 经验 0
  alice 房间[join] alice 进入房间 3
  alice 房间[join] bob 进入房间 3
  bob   状态[join] Lv1 HP 100/100 金币 0 经验 0
  bob   房间[join] bob 进入房间 3

=== 4. 开打：同组 peer 只 Post，结果靠回执 ===
  第 3 击
  bob   状态[damage] Lv1 HP 0/100 金币 0 经验 0
  bob   房间[report] alice 击倒了 bob
  alice 状态[combat] Lv2 HP 120/120 金币 30 经验 30
  alice 房间[report] alice 击倒了 bob
```

## 目录结构

```
cmd/engineering_example/
├── ids/                       全部 ActorId（唯一来源）
├── game/                      领域规则：纯函数，零框架依赖
├── protocol/                  面向客户端的报文（下行契约）
│
├── actors/
│   ├── world/                 实现包：State + RegisterHandlers + handler
│   │   └── api/               契约包：别人发给我、我发给别人的消息
│   ├── room/{,api/}
│   ├── player/{,api/}         grain：持久化 + 读 shared
│   └── client/{,api/}         连接：入口层 + 下行出口
│
├── setup/                     装配：容器 + 全部注册 + 世界初始化
├── demo/                      脚本化客户端（只依赖 ids + 契约包）
├── main.go                    入口：装配 → 跑剧本 → 优雅停机
└── integration_test.go        端到端用例
```

包分三类，规则只有一条：**依赖只能指向"契约"和"领域"，不能指向别的实现**。

| 包 | 职责 | 谁能 import |
|---|---|---|
| `ids` | 4 个 ActorId（`ClientId` / `PlayerId` / `RoomId` / `WorldId`）+ `TheWorld()` | 所有人 |
| `game` | 伤害 / 成长 / 奖励公式，纯函数，不认识 ActorId 也不认识 Manager | 实现包、测试 |
| `protocol` | 推给客户端的报文；`Msg` 用未导出方法封闭联合 | `client` / `player` |
| `actors/*/api` | **契约包**：请求类型（`ReqType`）+ 回复类型 + 对外数据 | 任何 actor 的实现包、`demo` |
| `actors/*` | **实现包**：State、handler、注册 | 只有 `setup` |
| `setup` | 装配；唯一同时 import 全部实现包的地方 | `main` |

`api` 包只依赖 `ids` / `protocol` / 别的 `api`，**绝不 import 实现包** —— 这是让
`player` 与 `room` 互相发消息却不产生循环依赖的唯一办法。中性命名会让 import 重名，
所以一律取别名：`playerapi "…/actors/player/api"`。

于是"新增一个 actor"的改动面被固定成两处：`ids` 加一个 ID 类型、`setup` 加一行注册。

## 演员表

| actor | 是什么 | 持久化 | 关键点 |
|---|---|---|---|
| `world` | 全服单例，持世界配置 | 否 | shared 容器的 **owner**：只有它能 Publish |
| `room` | 房间：成员摘要缓存 + 广播者 | 否 | 从不反向 Call 玩家；只做内存缓存 |
| `player` | 玩家的权威数据 | **grain** | 下行出口；从容器读世界配置 |
| `client` | 一条连接（≈ WebSocket session） | 否 | 入口层：只翻译形状、沿链转发 |

## 五条规则

### 1. 调用方向：只向右 Call，反向一律 Post

```
client ──Call──> player ──Call──> room
   ↑                │  ↑
   └──Post(下行)────┘  └──Post(事件／回执)── 同组 peer
```

- `Call` 只沿链向右（下游）：`client → player → room`。**反向 Call 与同类互 Call 一律是死锁**。
- 需要上游的信息？**在上游维护缓存**，不要反向取值。`room` 缓存成员摘要就是这么来的：
  玩家变更时 Post `BriefChanged` 上报，房间从不回查玩家。
- 同组 peer（玩家 A 与玩家 B）只 Post：A 等 B 回复、B 又要通知 A，就是 `A→B→A` 的环。

攻击回合是这条规则的样板：

```
alice(player) ──Post Damage──────────> bob(player)      // 只报"我打出了 40 攻击力"
bob(player)   ──Post CombatResult────> alice(player)    // 回执：掉了多少、死没死、我几级
alice(player) ──Post BriefChanged────> room             // 上报缓存
alice(player) ──Post Push(StateUpdate)> alice(client)    // 下行
```

### 2. 注册语义就是调度语义

`RegisterSpawn` / `RegisterQuery` / `RegisterServe` 的差别不是风格，而是**谁能把 actor 叫起来**：

| actor | 消息 | 注册 | 理由 |
|---|---|---|---|
| `client` | `Login` | Serve | 连接由第一条登录消息建立 |
| `player` | `Login` | Serve | 允许创建：只有登录能建出一个玩家 |
| `player` | 其余意图 / 事件 | Query | 打给不存在的玩家应当失败，而不是造一个 0 级空壳 |
| `room` | `Join` | Serve | 第一条 Join 就把房间建起来 |
| `room` | 其余 | Query | 房间不存在就没有"进错房"之外的语义 |
| `world` | `Init` | Spawn | 装配层启动时调一次，不需要再被查询 |

`integration_test.go` 的 `TestQueryDoesNotSpawn` 把这条钉住了：对不存在的玩家发请求必须
返回 `ActorNotFoundError`。

### 3. 下行：首帧全量，之后增量，而且一律 Post

- 新上场 / 重连必须由权威补一次**全量基准**（`protocol.LoginOK`），否则字段停在零值、
  不报错、也没人发现 —— "活着但变傻"。
- 之后只推变化（`protocol.StateUpdate`），带上 `Reason` 让客户端知道播哪段表现。
- 下行全走 `Post`：Post 不等送达，房间广播不会因为某个玩家卡住而拖垮全体。

副作用集中在 `player/player_notify.go` 一个文件里，handler 只表达"决定了什么"。

### 4. 持久化边界：只存"重启后还要的"

`PlayerState` 的字段分两段，区别就是这一点：

```go
type PlayerState struct {
    Level, HP, Gold, Exp int      // 持久化：角色成长
    Client ids.ClientId  `json:"-"` // 运行时：会话已失效，重连换一个新的
    Room   ids.RoomId    `json:"-"` // 运行时：房间成员表在房间那边，重启一起归零
    Vars   shared.Reader `json:"-"` // 运行时：世界配置重新订阅即可
}
```

落盘结果（`./data/Player/alice.json`）干净得能直接对：

```json
{ "generation": 1, "data": { "level": 2, "hp": 120, "gold": 30, "exp": 30 } }
```

`grain.SetupGrain` 托管了整个生命周期：激活（加载或新建）、按间隔存盘、退出时落盘并释放
租约。handler 里看不到 `Activate` / `Persist`。

### 5. 读多写少的全局事实走 shared 容器

世界配置每个玩家都要读，但几乎不变。让玩家 `Call world` 会把它变成扇入热点，
所以：

```
world ──PublishWait(ConfigKey)──> shared 容器 ──推送──> player 的 Reader 缓存
player ──Watch(ConfigKey)──────────────────────────────┘ （命中缓存，不触网）
```

- owner 用 `PublishWait`（要"已生效"的语义），读者用 `Watch`（首次订阅，之后吃缓存）。
- 权威数据始终在 `world` 的 State 里，容器只持副本。
- 测试里读到 `ExpRate == 200` 是**轮询**等来的：推送是异步的，最终一致。

## 三条链路

### 登录

```
demo ──Call Login────> client ──Call Login{Client}──> player
                                                     ├─ grain 激活（加载或新建）
                                                     ├─ 绑定下行出口
                                                     └─ Post Push(LoginOK) ──> client
```

### 聊天

```
client ──Call Say──> player ──Call Chat──> room ──AMulticast RoomEvent──> 每个成员 player
                                                                      └─ Post Push ──> client
```

房间是唯一的广播者，并且由它裁决"你是不是这个房间的成员" —— 客户端说什么不算数。

### 攻击（三击必杀）

Lv1 攻击力 40，对手 0 防御 → 每击 40 点：`100 → 60 → 20 → 倒地`。
奖励规则：命中 5 金币 / 10 经验，击杀换成一整份 20 / 40（按目标等级放大，不叠加）。
所以 alice 拿到 `10 + 10 + 40 = 60` 经验，1 级需要 30 → 升到 2 级余 30 经验、升级回满 120 血。

## 新增一个 actor 的步骤

以"加一个排行榜 actor"为例：

1. `ids` 加 `RankId`（`ActorType()` + `String()`）。
2. 建 `actors/rank/api/api.go`：写请求类型与 `ReqType`；需要回复就定义回复类型。
3. 建 `actors/rank/rank_state.go`（State + 领域方法）与 `rank_register.go`
   （`actor.Serve` + `RegisterXxx`；持久化就加 `grain.SetupGrain`，要读共享数据就加
   `shared.ServeReader`）。
4. `setup.New` 里加一行 `rank.RegisterHandlers(mgr)`。
5. 需要它通知别人时，把通知类型放进对应接收方的 `api` 包，用 `Post` 发出去。

## 与真实项目（erosion 服务端）的对照

| 本示例 | 真实项目 |
|---|---|
| `ids/` | `server/ids/` |
| `game/` | `server/game/`（领域层，不依赖协议与 actor） |
| `protocol/` | `server/protocol/` + `server/game/*`（由 codegen 产出客户端代码） |
| `actors/<x>/{,api/}` | `server/actorapp/<x>/` + `<x>_api/` |
| `setup/` | `server/actorapp/setup/` + `server/app/` |
| `client` actor | `session` actor（一条连接一个） |
| `demo/` | `test_client/`（e2e 测试客户端） |
| `WorldId{ID:"w0"}` 单例 | 世界 actor + 按 region 划分的 RegionActor |

差异只有规模：真实项目有 10 个 actor 包、上百条消息、按 region 分片。包的分法、
消息方向、首帧全量与缓存上报的纪律是一致的。

## 这个示例刻意不做的事

- **传输层**：没有 WebSocket，`client` actor 直接把报文塞进收件箱（`Drain` 取走）。
- **集群**：没有多节点、没有迁移。见 `../cluster_example`。
- **复活 / 离线托管 / 场景切换**：倒地即停，避免示例被"回合之外的状态机"淹没。
- **配置与数值表**：数值写死在 `game/combat.go`，真实项目从 YAML 加载。
