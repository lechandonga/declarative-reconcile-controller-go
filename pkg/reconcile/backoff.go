package reconcile

import (
	"math"
	"time"
)

// Backoff computes capped exponential retry delays. There is no random
// jitter: determinism makes tests and logs reproducible. attempt is 1-based
// (1 = delay after the first failure).
type Backoff struct {
	// Base is the delay after the first failure.
	Base time.Duration
	// Factor doubles the delay each attempt (2.0 => 1s, 2s, 4s, ...).
	Factor float64
	// Max caps the delay.
	Max time.Duration
}

// DefaultBackoff is used by the controller unless overridden.
var DefaultBackoff = Backoff{Base: 100 * time.Millisecond, Factor: 2, Max: 5 * time.Second}

// Delay returns the wait before retry number `attempt` (1-based).
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(b.Base) * math.Pow(b.Factor, float64(attempt-1))
	if d > float64(b.Max) || d != d { // guard NaN
		d = float64(b.Max)
	}
	return time.Duration(d)
}
