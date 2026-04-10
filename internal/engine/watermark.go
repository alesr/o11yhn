package engine

import "sync"

type watermarkTracker struct {
	mu       sync.Mutex
	lastTime int64
}

func newWatermarkTracker(initial int64) *watermarkTracker {
	return &watermarkTracker{lastTime: initial}
}

func (w *watermarkTracker) MarkProcessed(lastTime int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if lastTime > w.lastTime {
		w.lastTime = lastTime
	}
}

func (w *watermarkTracker) LastTime() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastTime
}
