package pubsub

import (
	"sync"
)

func newWaiter() *waiters {
	return &waiters{
		list: make(map[string]chan struct{}),
	}
}

type waiters struct {
	rw   sync.RWMutex
	list map[string]chan struct{}
}

func (w *waiters) Wait() {
	wait := make(chan struct{})
	w.rw.Lock()
	w.list["wait"] = wait
	w.rw.Unlock()
	<-wait
}

func (w *waiters) Done() {
	w.rw.Lock()
	defer w.rw.Unlock()
	for k, item := range w.list {
		item <- struct{}{}
		delete(w.list, k)
	}
}
