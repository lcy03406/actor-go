package actor

type closeInvoke[A ActorId, S anyState] struct {
}

func (i *closeInvoke[A, S]) Allow(id A, spawning bool) bool {
	return true
}

func (i *closeInvoke[A, S]) Invoke(actor *ActorContext[A, S], spawning bool) {
}

func (i *closeInvoke[A, S]) Fail(err error) {
}
