package logic

import (
	"testing"
)

func TestFindPath_StraightLine(t *testing.T) {
	g := NewGrid(5, 5)
	result := FindPath(g, Point{0, 0}, Point{4, 0})
	if !result.Found {
		t.Fatal("应该找到路径")
	}
	if len(result.Path) != 5 {
		t.Fatalf("期望路径长度=5, 实际=%d", len(result.Path))
	}
	if result.Distance != 4 {
		t.Fatalf("期望距离=4, 实际=%f", result.Distance)
	}
}

func TestFindPath_AroundWall(t *testing.T) {
	g := NewGrid(3, 3)
	g.SetWall(1, 1) // 中间是墙

	result := FindPath(g, Point{0, 0}, Point{2, 2})
	if !result.Found {
		t.Fatal("应该绕开墙找到路径")
	}
	// 路径不能经过 (1,1)
	for _, p := range result.Path {
		if p.X == 1 && p.Y == 1 {
			t.Fatal("路径不应经过墙壁")
		}
	}
}

func TestFindPath_NoPath(t *testing.T) {
	g := NewGrid(3, 1)
	g.SetWall(1, 0) // 唯一通道被堵

	result := FindPath(g, Point{0, 0}, Point{2, 0})
	if result.Found {
		t.Fatal("不应找到路径")
	}
}

func TestFindPath_StartBlocked(t *testing.T) {
	g := NewGrid(3, 3)
	g.SetWall(0, 0)
	result := FindPath(g, Point{0, 0}, Point{2, 2})
	if result.Found {
		t.Fatal("起点被堵，不应找到路径")
	}
}

func TestFindPath_EndBlocked(t *testing.T) {
	g := NewGrid(3, 3)
	g.SetWall(2, 2)
	result := FindPath(g, Point{0, 0}, Point{2, 2})
	if result.Found {
		t.Fatal("终点被堵，不应找到路径")
	}
}

func TestFindPath_OutOfBounds(t *testing.T) {
	g := NewGrid(3, 3)
	result := FindPath(g, Point{-1, 0}, Point{5, 0})
	if result.Found {
		t.Fatal("越界坐标不应找到路径")
	}
}

// ─── 距离计算 ───

func TestManhattanDist(t *testing.T) {
	if d := ManhattanDist(Point{0, 0}, Point{3, 4}); d != 7 {
		t.Fatalf("期望=7, 实际=%d", d)
	}
	if d := ManhattanDist(Point{5, 2}, Point{3, 5}); d != 5 {
		t.Fatalf("期望=5, 实际=%d", d)
	}
}

func TestInRange(t *testing.T) {
	if !InRange(Point{0, 0}, Point{3, 2}, 5) {
		t.Fatal("距离=5 应该在范围内")
	}
	if InRange(Point{0, 0}, Point{3, 3}, 5) {
		t.Fatal("距离=6 不应在范围内")
	}
}

func TestEuclideanDist(t *testing.T) {
	d := EuclideanDist(Point{0, 0}, Point{3, 4})
	if d < 4.9 || d > 5.1 {
		t.Fatalf("期望≈5.0, 实际=%f", d)
	}
}
