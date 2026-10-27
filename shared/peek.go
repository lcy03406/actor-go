package shared

// Peek 只读本地缓存的快照：命中返回 (v, true)，未命中（尚未订阅 / TTL 过期）返回 false。
//
// 与 Watch 的分工：Watch 负责"值怎么来"（未命中就向容器订阅，可能阻塞一次 Call）；
// Peek 负责"值怎么读"（绝不注册、绝不联网，未命中即失败信号，由调用方决定降级）。
// 因此它适合热路径读取——读取本身不会改变订阅状态，也不会把一次读放大成一次 RTT。
//
// 语义上是 cacheGet 的导出形态：失效判据完全一致（未加载 + TTL 过期），
// 只是去掉了 Watch 的"未命中就去注册"副作用。
//
// 与 Reader 的其它用法一致，只允许在所属 actor 的 run goroutine 内调用。
func Peek[V any](r *Reader, k Key[V]) (V, bool) {
	return cacheGet(r, k)
}
