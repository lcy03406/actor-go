package grain

// Snapshotter 是快照转换器接口，负责在业务数据 D 与持久化快照 P 之间转换。
// Driver 在 Save/Load 时调用它完成快照的创建与回放：
//   - NewPersist：基于当前数据构造一个待持久化的快照对象
//   - LoadSnapshot：将一个已加载的快照写回到业务数据 D 中
//   - TakeSnapshot：基于当前业务数据生成快照对象（用于实际落盘）
type Snapshotter[D any, P any] interface {
	NewPersist(data *D) *P
	LoadSnapshot(data *D, persist *P)
	TakeSnapshot(data *D) *P
}

// ShotSelf 是一个内置的 Snapshotter 实现：快照类型就是业务数据自身（P == D），
// 即整份数据直接作为快照保存与加载，不做任何转换。适用于无需裁剪/转换的简单场景。
type ShotSelf[D any] struct{}

func (s *ShotSelf[D]) NewPersist(data *D) *D {
	return data
}

func (s *ShotSelf[D]) LoadSnapshot(data *D, persist *D) {
	if persist == nil || persist == data {
		return
	}
	*data = *persist
}

func (s *ShotSelf[D]) TakeSnapshot(data *D) *D {
	return data
}
