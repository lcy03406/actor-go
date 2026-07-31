package grain

type Snapshotter[D any, P any] interface {
	NewPersist(data *D) *P
	LoadSnapshot(data *D, persist *P)
	TakeSnapshot(data *D) *P
}

type ShotSelf[D any] struct{}

func (s *ShotSelf[D]) NewPersist(data *D) *D {
	return data
}

func (s *ShotSelf[D]) LoadSnapshot(data *D, persist *D) {
	if persist == data {
		return
	}
	*data = *persist
}

func (s *ShotSelf[D]) TakeSnapshot(data *D) *D {
	return data
}
