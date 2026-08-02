package logic

import (
	"testing"
)

// ─── CalcDamage ───

func TestCalcDamage_Normal(t *testing.T) {
	r := CalcDamage(100, 30, 1, 20, 0.5, 10, 0)
	// raw = 20 + 100*0.5 + 1*10 = 80
	// mit = 80 - 30*0.3 = 71
	if r.Raw != 80 {
		t.Fatalf("期望 Raw=80, 实际=%d", r.Raw)
	}
	if r.Mitigated != 71 {
		t.Fatalf("期望 Mitigated=71, 实际=%d", r.Mitigated)
	}
	if r.Critical {
		t.Fatal("不应暴击")
	}
}

func TestCalcDamage_Minimum(t *testing.T) {
	r := CalcDamage(0, 1000, 1, 0, 0, 0, 0)
	if r.Mitigated < 1 {
		t.Fatalf("伤害不应低于1, 实际=%d", r.Mitigated)
	}
}

func TestCalcDamage_Critical(t *testing.T) {
	// atk=100 def=30 level=10 base=20 coef=0.5 growth=10 critChance=0.5
	// raw = 20+50+100=170, mit=170-9=161, 暴击=161*1.5=241
	r := CalcDamage(100, 30, 10, 20, 0.5, 10, 0.5)
	if !r.Critical {
		t.Fatal("应该暴击")
	}
	if r.Mitigated != 241 {
		t.Fatalf("暴击伤害应为241, 实际=%d", r.Mitigated)
	}
}

func TestCalcDamage_HighLevelBonus(t *testing.T) {
	// 验证技能等级对伤害的影响
	r1 := CalcDamage(50, 20, 1, 10, 1.0, 5, 0)
	r2 := CalcDamage(50, 20, 10, 10, 1.0, 5, 0)
	if r2.Mitigated <= r1.Mitigated {
		t.Fatal("高等级技能伤害应更高")
	}
}

// ─── CalcHeal ───

func TestCalcHeal(t *testing.T) {
	if h := CalcHeal(100, 50); h != 150 {
		t.Fatalf("期望=150, 实际=%d", h)
	}
}

// ─── CalcExpToLevel ───

func TestCalcExpToLevel(t *testing.T) {
	tests := []struct {
		level int
		exp   int
	}{
		{1, 100},
		{2, 200},
		{10, 1000},
	}
	for _, tt := range tests {
		if got := CalcExpToLevel(tt.level); got != tt.exp {
			t.Errorf("Level=%d 期望=%d 实际=%d", tt.level, tt.exp, got)
		}
	}
}
