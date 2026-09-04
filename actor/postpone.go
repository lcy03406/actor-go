package actor

type Postpone struct {
	Queue []func() error
}
