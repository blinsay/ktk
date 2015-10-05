package producer

import (
	"time"
)

type Throttle interface {
	Await()
}

// A throttle that always returns immediately from Await. For testing.
type noOpThrottle struct{}

func (n *noOpThrottle) Await() {}

// A throttle that does exponential backoff with a maximum setting.
type exponentialThrottle struct {
	waitFor int
	maxWait int
	unit    time.Duration
}

func (e *exponentialThrottle) Await() {
	time.Sleep(time.Duration(e.waitFor) * e.unit)
	e.waitFor = intMin(2*e.waitFor, e.maxWait)
}
