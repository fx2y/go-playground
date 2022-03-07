package cloud_native

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Circuit func(ctx context.Context) (string, error)

func Breaker(circuit Circuit, failureThreshold uint) Circuit {
	var consecutiveFailures = 0
	var lastAttempt = time.Now()
	var m sync.RWMutex

	return func(ctx context.Context) (string, error) {
		m.RLock() // Establish a "read lock"

		d := consecutiveFailures - int(failureThreshold)

		if d >= 0 {
			shouldRetryAt := lastAttempt.Add(time.Second * 2 << d)
			if !time.Now().After(shouldRetryAt) {
				m.RUnlock()
				return "", errors.New("service unreachable")
			}
		}

		m.RUnlock() // Release read lock

		response, err := circuit(ctx) // Issue request proper

		m.Lock()
		defer m.Unlock()

		lastAttempt = time.Now()

		if err != nil {
			consecutiveFailures++
			return response, err
		}

		consecutiveFailures = 0 // Reset failures counter

		return response, nil
	}
}

func DebounceFirst(circuit Circuit, d time.Duration) Circuit {
	var threshold time.Time
	var result string
	var err error
	var m sync.Mutex

	return func(ctx context.Context) (string, error) {
		m.Lock()

		defer func() {
			threshold = time.Now().Add(d)
			m.Unlock()
		}()

		if time.Now().Before(threshold) {
			return result, err
		}

		result, err = circuit(ctx)

		return result, err
	}
}

func DebounceLast(circuit Circuit, d time.Duration) Circuit {
	var threshold = time.Now()
	var ticker *time.Ticker
	var result string
	var err error
	var once sync.Once
	var m sync.Mutex

	return func(ctx context.Context) (string, error) {
		m.Lock()
		defer m.Unlock()

		threshold = time.Now().Add(d)

		once.Do(func() {
			ticker = time.NewTicker(time.Millisecond * 100)

			go func() {
				defer func() {
					m.Lock()
					ticker.Stop()
					once = sync.Once{}
					m.Unlock()
				}()

				for {
					select {
					case <-ticker.C:
						m.Lock()
						if time.Now().After(threshold) {
							result, err = circuit(ctx)
							m.Unlock()
							return
						}
						m.Unlock()
					case <-ctx.Done():
						m.Lock()
						result, err = "", ctx.Err()
						m.Unlock()
						return
					}
				}
			}()
		})

		return result, err
	}
}
