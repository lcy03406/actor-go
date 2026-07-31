package actor

import "time"

type timerInvoke[A ActorId, S anyState] struct {
	fn func()
	id int
	t  *time.Timer
}

func (i *timerInvoke[A, S]) Allow(id A, spawning bool) bool {
	return !spawning
}

func (i *timerInvoke[A, S]) Invoke(actor *ActorContext[A, S], spawning bool) {
	timer, ok := actor.timers[i.id]
	if !ok || timer != i.t {
		return
	}
	delete(actor.timers, i.id)
	defer func() {
		if r := recover(); r != nil {
			id := actor.Id()
			actor.Logger().Warn("timer invoke panic", "id", id, "panic", r)
		}
	}()
	i.fn()
}

func (i *timerInvoke[A, S]) Fail(err error) {
}
