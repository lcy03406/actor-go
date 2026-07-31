package cluster

// RouteDecision 表示路由决策结果。
type RouteDecision int

const (
	// RouteLocal 消息由本地节点处理。
	RouteLocal RouteDecision = iota
	// RouteForward 消息需要转发到目标节点。
	// 用于 Serve 模式（allowSpawn && allowQuery）的自动重定向。
	RouteForward
	// RouteFail 消息无法路由，调用方应处理错误。
	// 用于 Spawn 模式（仅 allowSpawn）和 Query 模式（仅 allowQuery）的显式路由失败。
	RouteFail
)

// RouteResult 是路由决策的结果。
type RouteResult struct {
	Decision RouteDecision
	// Target 是目标节点（仅在 Decision 为 RouteForward 时有效）。
	Target Node
	// Err 是路由错误信息（仅在 Decision 为 RouteFail 时有效）。
	Err *RouteError
}

// Route 对一条消息做路由决策。
//
// 规则：
//   - 偏好节点 == 本地 → RouteLocal
//   - 偏好节点 != 本地 && allowSpawn && allowQuery（Serve 模式）→ RouteForward
//   - 偏好节点 != 本地 && 非 Serve 模式 → RouteFail
//
// Serve 模式自动重定向的理由：这是最常见的模式，消息既可能 spawn 也可能 query，
// 调用方不应关心 Actor 当前在哪个节点上。
//
// Spawn/Query 模式暴露路由错误的理由：Spawn 是重操作（涉及 lease、状态加载），
// 调用方应显式指定目标节点；Query 假设 Actor 已在某节点运行，路由失败说明状态不一致。
func Route(
	self Node,
	preferred Node,
	allowSpawn bool,
	allowQuery bool,
) RouteResult {
	if preferred.ID == self.ID {
		return RouteResult{Decision: RouteLocal}
	}

	if allowSpawn && allowQuery {
		// Serve 模式：自动转发
		return RouteResult{Decision: RouteForward, Target: preferred}
	}

	// Spawn 或 Query 模式：暴露错误
	return RouteResult{
		Decision: RouteFail,
		Err: &RouteError{
			ActorType:  "",
			ActorId:    "",
			Owner:      preferred.ID,
			AllowSpawn: allowSpawn,
			AllowQuery: allowQuery,
		},
	}
}

// RouteError 表示路由失败，携带目标节点信息供调用方处理。
type RouteError struct {
	ActorType  string
	ActorId    string
	Owner      string // 偏好节点 ID
	AllowSpawn bool
	AllowQuery bool
}

func (e *RouteError) Error() string {
	msg := "route error: actor " + e.ActorType + ":" + e.ActorId
	if e.AllowSpawn && !e.AllowQuery {
		msg += " (spawn-only) should be on node " + e.Owner
	} else if !e.AllowSpawn && e.AllowQuery {
		msg += " (query-only) not found locally, owner is " + e.Owner
	} else {
		msg += " not routable, owner is " + e.Owner
	}
	return msg
}

// IsRouteError 判断 error 是否为路由错误。
func IsRouteError(err error) (*RouteError, bool) {
	re, ok := err.(*RouteError)
	return re, ok
}
