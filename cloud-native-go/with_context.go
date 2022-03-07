package cloud_native

import "context"

type Function func(string) (string, error)

type WithContext func(context.Context, string) (string, error)

func Timeout(f Function) WithContext {
	return func(ctx context.Context, arg string) (string, error) {
		chres := make(chan string)
		cherr := make(chan error)

		go func() {
			res, err := f(arg)
			chres <- res
			cherr <- err
		}()

		select {
		case res := <-chres:
			return res, <-cherr
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}