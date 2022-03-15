package pkg

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	stateRunning uint32 = 0
	stateStopped uint32 = 1
)

var (
	// ErrStopped when stopped
	errStopped = errors.New("WorkerPool already stopped")
)

type workerPool struct {
	inNums     uint64
	outNums    uint64
	curWorkers int

	maxWorkers int
	workChan   chan func()

	taskDone *sync.Cond
	state    uint32
	sync.Mutex
}

func newWorkerPool(maxWorkers int, queueSize int) *workerPool {
	if maxWorkers <= 0 {
		logger.Fatal("WorkerNum must be greater than zero")
	}
	if queueSize <= 0 {
		logger.Fatal("queueSize must be greater than zero")
	}

	w := &workerPool{
		maxWorkers: maxWorkers,
		workChan:   make(chan func(), queueSize),
	}

	w.taskDone = sync.NewCond(w)

	w.start()
	return w
}

func (w *workerPool) workerFunc() {
	w.Lock()
	w.curWorkers++
	w.Unlock()
LOOP:
	for fn := range w.workChan {
		fn()
		var needQuit bool
		w.Lock()
		w.outNums++
		if w.inNums == w.outNums {
			w.taskDone.Signal()
		}
		if w.curWorkers > w.maxWorkers {
			w.curWorkers--
			needQuit = true
		}
		w.Unlock()
		if needQuit {
			break LOOP
		}
	}
}

func (w *workerPool) start() {
	for i := 0; i < w.maxWorkers; i++ {
		go w.workerFunc()
	}
}

func (w *workerPool) submit(fn func()) (err error) {
	if atomic.LoadUint32(&w.state) == stateStopped {
		return errStopped
	}

	w.Lock()
	w.inNums++
	w.Unlock()

	w.workChan <- fn
	return nil
}
