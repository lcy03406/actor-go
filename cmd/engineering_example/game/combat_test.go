package game

import "testing"

func TestDamage(t *testing.T) {
	cases := []struct {
		name     string
		atk, def int
		want     int
	}{
		{"扣防御", 40, 5, 35},
		{"同归于零时保底 1 点", 10, 10, 1},
		{"防御高于攻击时保底 1 点", 5, 30, 1},
		{"负防御等于加伤", 40, -10, 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Damage(c.atk, c.def); got != c.want {
				t.Fatalf("Damage(%d, %d) = %d，期望 %d", c.atk, c.def, got, c.want)
			}
		})
	}
}

func TestLevelCurve(t *testing.T) {
	if MaxHPOf(1) != 100 || MaxHPOf(2) != 120 {
		t.Fatalf("生命曲线不符: %d / %d", MaxHPOf(1), MaxHPOf(2))
	}
	if AtkOf(1) != 40 || AtkOf(2) != 45 {
		t.Fatalf("攻击曲线不符: %d / %d", AtkOf(1), AtkOf(2))
	}
	if DefOf(1) != 0 || DefOf(3) != 2 {
		t.Fatalf("防御曲线不符: %d / %d", DefOf(1), DefOf(3))
	}
	if ExpToLevel(1) != 30 || ExpToLevel(2) != 60 {
		t.Fatalf("经验曲线不符: %d / %d", ExpToLevel(1), ExpToLevel(2))
	}
}

// TestReward 钉住"击杀换整份、不叠加"的结算规则：
// 规则一变，集成用例里的升级断言就会跟着失败 —— 这是刻意的联动。
func TestReward(t *testing.T) {
	gold, exp := Reward(false, 1)
	if gold != 5 || exp != 10 {
		t.Fatalf("命中奖励不符: %d / %d", gold, exp)
	}
	gold, exp = Reward(true, 3)
	if gold != 60 || exp != 120 {
		t.Fatalf("击杀奖励应随目标等级放大: %d / %d", gold, exp)
	}
}
