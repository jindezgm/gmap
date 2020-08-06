/*
 * @Author: jinde.zgm
 * @Date: 2020-07-29 05:28:54
 * @Descripttion:
 */

package gmap

// watcher implement Watcher interfaces.
type watcher struct {
	m      *typedMap     // Map pointer
	eventc chan MapEvent // Input map event
	watchc chan MapEvent // Output map event
	stopc  chan struct{} // Close flag.
	done   chan struct{} // Closed flag.
}

var _ Watcher = &watcher{}

// newWatcher create watcher.
func newWatcher(m *typedMap) *watcher {
	// Create watcher object.
	w := &watcher{
		m:      m,
		eventc: make(chan MapEvent),
		watchc: make(chan MapEvent),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	// Watch map.
	go w.run()

	return w
}

// WatchChan implement Watcher.WatchChan()
func (w *watcher) WatchChan() <-chan MapEvent { return w.watchc }

// Close implement Watcher.Close().
func (w *watcher) Close() {
	// Remove watcher from map.
	w.m.watchers.Delete(w)
	// Send signal to watcher coroutine.
	close(w.stopc)
	// Waiting for watcher coroutine exist.
	<-w.done
}

// send event to watcher.
func (w *watcher) send(e MapEvent) {
	select {
	case w.eventc <- e:
	case <-w.stopc:
	}
}

// run watch map event and output event by channel.
func (w *watcher) run() {
	defer close(w.done)

	// events is event buffer.
	events := []MapEvent{}

	for {
		var event MapEvent
		var watchc chan<- MapEvent
		// Output by watchc only when there are events in the buffer
		if len(events) > 0 {
			event, watchc = events[0], w.watchc
		}

		select {
		// New event.
		case e := <-w.eventc:
			events = append(events, e)
		// Event is outputted
		case watchc <- event:
			events = events[1:]
		// Watcher is closed.
		case <-w.stopc:
			return
		}
	}
}
