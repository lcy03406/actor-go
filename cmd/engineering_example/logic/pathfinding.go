// pathfinding.go 寻路算法（A*）。
// 纯计算逻辑，不依赖任何 actor 框架包。
package logic

import (
	"math"
)

// ─── 坐标与网格 ───

// Point 二维坐标点。
type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

// Grid 寻路网格（true 表示可行走）。
type Grid struct {
	Width  int
	Height int
	Walls  map[Point]bool // true 表示障碍物
}

// NewGrid 创建新网格。
func NewGrid(width, height int) *Grid {
	return &Grid{
		Width:  width,
		Height: height,
		Walls:  make(map[Point]bool),
	}
}

// SetWall 设置障碍物。
func (g *Grid) SetWall(x, y int) {
	g.Walls[Point{X: x, Y: y}] = true
}

// IsWalkable 判断是否可行走。
func (g *Grid) IsWalkable(p Point) bool {
	if p.X < 0 || p.X >= g.Width || p.Y < 0 || p.Y >= g.Height {
		return false
	}
	return !g.Walls[p]
}

// ─── A* 寻路 ───

// PathNode A* 节点。
type pathNode struct {
	pos  Point
	g, h float64
	prev *pathNode
}

func (n *pathNode) f() float64 { return n.g + n.h }

// PathResult 寻路结果。
type PathResult struct {
	Path     []Point `json:"path"`
	Distance float64 `json:"distance"` // 路径总长度
	Found    bool    `json:"found"`
}

// FindPath A* 寻路。
//
// 使用曼哈顿距离作为启发式，返回从 start 到 end 的最短路径。
func FindPath(grid *Grid, start, end Point) PathResult {
	if !grid.IsWalkable(start) || !grid.IsWalkable(end) {
		return PathResult{Found: false}
	}

	openList := []*pathNode{{pos: start, h: heuristic(start, end)}}
	closed := make(map[Point]bool)

	for len(openList) > 0 {
		// 找 f 值最小的节点
		minIdx := 0
		for i := range openList {
			if openList[i].f() < openList[minIdx].f() {
				minIdx = i
			}
		}
		current := openList[minIdx]
		openList = append(openList[:minIdx], openList[minIdx+1:]...)

		if current.pos == end {
			return buildResult(current)
		}

		closed[current.pos] = true

		for _, dir := range []Point{{0, 1}, {0, -1}, {1, 0}, {-1, 0}} {
			neighbor := Point{current.pos.X + dir.X, current.pos.Y + dir.Y}
			if !grid.IsWalkable(neighbor) || closed[neighbor] {
				continue
			}

			g := current.g + 1
			h := heuristic(neighbor, end)

			// 检查 openList 中是否已有更优路径
			skip := false
			for _, n := range openList {
				if n.pos == neighbor && n.g <= g {
					skip = true
					break
				}
			}
			if skip {
				continue
			}

			openList = append(openList, &pathNode{pos: neighbor, g: g, h: h, prev: current})
		}
	}

	return PathResult{Found: false}
}

func heuristic(a, b Point) float64 {
	return math.Abs(float64(a.X-b.X)) + math.Abs(float64(a.Y-b.Y))
}

func buildResult(node *pathNode) PathResult {
	var path []Point
	dist := node.g
	for node != nil {
		path = append([]Point{node.pos}, path...)
		node = node.prev
	}
	return PathResult{Path: path, Distance: dist, Found: true}
}

// ─── 距离计算 ───

// ManhattanDist 曼哈顿距离。
func ManhattanDist(a, b Point) int {
	dx := a.X - b.X
	dy := a.Y - b.Y
	if dx < 0 {
		dx = -dx
	}
	if dy < 0 {
		dy = -dy
	}
	return dx + dy
}

// EuclideanDist 欧几里得距离。
func EuclideanDist(a, b Point) float64 {
	return math.Sqrt(float64((a.X-b.X)*(a.X-b.X) + (a.Y-b.Y)*(a.Y-b.Y)))
}

// InRange 判断两点是否在指定距离范围内。
func InRange(a, b Point, maxDist int) bool {
	return ManhattanDist(a, b) <= maxDist
}
