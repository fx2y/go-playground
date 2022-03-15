package pkg

import "go.uber.org/zap"

var (
	globalWritingPool *workerPool
	logger            *zap.Logger
)

func initGlobalWritingPool(maxWorkers int) {
	if globalWritingPool != nil {
		return
	}
	queueSize := 3
	globalWritingPool = newWorkerPool(maxWorkers, queueSize)
	logger.Info("initialized writing pool", zap.Int("maxWorkers", maxWorkers), zap.Int("queueSize", queueSize))
}
