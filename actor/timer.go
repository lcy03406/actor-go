package actor

import "time"

type timerStub struct {
	fn func()
	id TimerId
	t  *time.Timer
}

type timerInvoke[A ActorId, S anyState] struct {
	*timerStub
}

func (i timerInvoke[A, S]) Allow(id A, spawning bool) bool {
	return !spawning
}

func (i timerInvoke[A, S]) Invoke(actor *ActorContext[A, S], spawning bool) {
	timer, ok := actor.ctrl.timers[i.id]
	if !ok || timer != i.t {
		return
	}
	delete(actor.ctrl.timers, i.id)
	defer func() {
		if r := recover(); r != nil {
			id := actor.Id()
			actor.Logger().Warn("timer invoke panic", "id", id, "panic", r)
		}
	}()
	i.fn()
}

func (i timerInvoke[A, S]) Fail(err error) {
}
