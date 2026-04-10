package engine

import "sync"

type idDeduper struct {
	mu        sync.Mutex
	processed map[int]int64
	pending   map[int]int64
	failed    map[int]int64
}

func newIDDeduper() *idDeduper {
	return &idDeduper{
		processed: make(map[int]int64),
		pending:   make(map[int]int64),
		failed:    make(map[int]int64),
	}
}

func (d *idDeduper) markPending(id int, createdAt int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.pending[id]; ok {
		return false
	}

	d.pending[id] = createdAt
	return true
}

func (d *idDeduper) markProcessed(id int, createdAt int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.pending, id)
	delete(d.failed, id)

	if current, ok := d.processed[id]; !ok || createdAt > current {
		d.processed[id] = createdAt
	}
}

func (d *idDeduper) markFailed(id int, createdAt int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, id)
	d.failed[id] = createdAt
}

func (d *idDeduper) retrySince(defaultSince int64) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	since := defaultSince
	for _, createdAt := range d.failed {
		candidate := max(createdAt-1, 0)

		if candidate < since {
			since = candidate
		}
	}
	return since
}

func (d *idDeduper) seenInWindow(id int, cutoff int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if createdAt, ok := d.pending[id]; ok && createdAt >= cutoff {
		return true
	}

	createdAt, ok := d.processed[id]
	if !ok {
		return false
	}
	return createdAt >= cutoff
}

func (d *idDeduper) prune(cutoff int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for id, createdAt := range d.processed {
		if createdAt < cutoff {
			delete(d.processed, id)
		}
	}

	// do not prune pending entries by timestamp
	// they are in-flight work and must be released only via markProcessed / markFailed.
}
