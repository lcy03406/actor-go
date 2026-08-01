// Package game 是与 actor 框架完全无关的领域规则：纯函数、无状态、可独立单测。
//
// 它既不认识 ActorId 也不认识 Manager；状态变更由 actors/* 调用这些函数完成。
// 依赖方向始终是 actors → game，反向禁止。
package game

// 基础数值：小到可以一眼验算，够演示用。
const (
	BaseHP      = 100
	HPPerLevel  = 20
	BaseAtk     = 40
	AtkPerLevel = 5
	BaseDef     = 0
	DefPerLevel = 1
)

// Damage 一次攻击的实际伤害：攻击力减防御，至少 1 点。
func Damage(atk, def int) int {
	if d := atk - def; d > 1 {
		return d
	}
	return 1
}

// MaxHPOf 由等级换算生命上限。
func MaxHPOf(level int) int { return BaseHP + HPPerLevel*(level-1) }

// AtkOf 由等级换算攻击力。
func AtkOf(level int) int { return BaseAtk + AtkPerLevel*(level-1) }

// DefOf 由等级换算防御力。
func DefOf(level int) int { return BaseDef + DefPerLevel*(level-1) }

// ExpToLevel 升到下一级所需经验。
func ExpToLevel(level int) int { return 30 * level }

// HitReward 命中的小额奖励（金币, 经验）。
func HitReward() (gold, exp int) { return 5, 10 }

// KillReward 击杀奖励（金币, 经验）。
func KillReward(level int) (gold, exp int) { return 20 * level, 40 * level }

// Reward 结算一次命中的奖励：击杀把命中奖励整份换成 KillReward（不叠加）。
func Reward(slain bool, targetLevel int) (gold, exp int) {
	if slain {
		return KillReward(targetLevel)
	}
	return HitReward()
}
