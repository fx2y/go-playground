package pkg

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"strings"
	"sync"
	"time"
)

var (
	replicaSpecificErrorCodes = []int32{242, 319, 1000}
)

type clickHouse struct {
	dims       []*columnWithType
	cfg        *config
	taskCfg    *taskConfig
	prepareSQL string

	numFlying int32
	mux       sync.Mutex
	taskDone  *sync.Cond
}

func newClickHouse(cfg *config, taskCfg *taskConfig) *clickHouse {
	ck := &clickHouse{cfg: cfg, taskCfg: taskCfg}
	ck.taskDone = sync.NewCond(&ck.mux)
	return ck
}

func (c *clickHouse) init() (err error) {
	return c.initSchema()
}

func (c *clickHouse) drain() {
	c.mux.Lock()
	for c.numFlying != 0 {
		logger.Debug("draining flying batches",
			zap.String("task", c.taskCfg.name),
			zap.Int32("pending", c.numFlying))
		c.taskDone.Wait()
	}
	c.mux.Unlock()
}

func (c *clickHouse) send(batch *batch) {
	c.mux.Lock()
	c.numFlying++
	c.mux.Unlock()
	_ = globalWritingPool.submit(func() {
		c.loopWrite(batch)
		c.mux.Lock()
		c.numFlying--
		if c.numFlying == 0 {
			c.taskDone.Broadcast()
		}
		c.mux.Unlock()
	})
}

func (c *clickHouse) write(batch *batch, sc *shardConn, dbVer *int) (err error) {
	if len(*batch.rows) == 0 {
		return
	}
	var conn *sql.DB
	if conn, *dbVer, err = sc.nextGoodReplica(*dbVer); err != nil {
		return
	}
	numDims := len(c.dims)

	if _, err = writeRows(c.prepareSQL, *batch.rows, 0, numDims, conn); err != nil {
		return
	}
	return
}

func (c *clickHouse) loopWrite(batch *batch) {
	var err error
	var times int
	var reconnect bool
	var dbVer int
	sc := getShardConn(batch.batchIdx)
	for {
		if err = c.write(batch, sc, &dbVer); err == nil {
			if err = batch.commit(); err == nil {
				return
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe) {
				logger.Warn("Batch.commit failed due to the context has been cancelled", zap.String("task", c.taskCfg.name))
				return
			}
			logger.Fatal("Batch.commit failed with permanent error", zap.String("task", c.taskCfg.name), zap.Error(err))
		}
		if errors.Is(err, context.Canceled) {
			logger.Info("clickHouse.write failed due to the context has been cancelled", zap.String("task", c.taskCfg.name))
			return
		}
		logger.Error("flush batch failed", zap.String("task", c.taskCfg.name), zap.Int("try", times), zap.Error(err))
		times++
		reconnect = shouldReconnect(err, sc)
		if reconnect && (c.cfg.clickhouse.retryTimes <= 0 || times < c.cfg.clickhouse.retryTimes) {
			time.Sleep(10 * time.Second)
		} else {
			logger.Fatal("clickHouse.loopWrite failed", zap.String("task", c.taskCfg.name))
		}
	}
}

func (c *clickHouse) initSchema() (err error) {
	c.dims = make([]*columnWithType, 0)
	for _, dim := range c.taskCfg.dims {
		tp, nullable := whichType(dim.typ)
		c.dims = append(c.dims, &columnWithType{
			name:       dim.name,
			typ:        tp,
			nullable:   nullable,
			sourceName: dim.sourceName,
		})
	}

	numDims := len(c.dims)
	quotedDims := make([]string, numDims)
	params := make([]string, numDims)
	for i := 0; i < numDims; i++ {
		quotedDims[i] = fmt.Sprintf("`%s`", c.dims[i].name)
		params[i] = "?"
	}
	c.prepareSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		c.cfg.clickhouse.db,
		c.taskCfg.tableName,
		strings.Join(quotedDims, ","),
		strings.Join(params, ","))
	logger.Info(fmt.Sprintf("Prepare sql=> %s", c.prepareSQL), zap.String("task", c.taskCfg.name))

	return nil
}

func shouldReconnect(err error, sc *shardConn) bool {
	var exp *clickhouse.Exception
	if errors.As(err, &exp) {
		logger.Error("this is an exception from clickhouse-server", zap.String("dsn", sc.getDsn()), zap.Reflect("exception", exp))
		var replicaSpecific bool
		for _, ec := range replicaSpecificErrorCodes {
			if ec == exp.Code {
				replicaSpecific = true
				break
			}
		}
		return replicaSpecific
	}
	return true
}

func writeRows(prepareSQL string, rows rows, idxBegin, idxEnd int, conn *sql.DB) (numBad int, err error) {
	var stmt *sql.Stmt
	var tx *sql.Tx
	var errExec error
	if tx, err = conn.Begin(); err != nil {
		err = errors.Wrapf(err, "conn.Begin %s", prepareSQL)
		return
	}
	if stmt, err = tx.Prepare(prepareSQL); err != nil {
		err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
		_ = tx.Rollback()
		return
	}
	defer stmt.Close()
	var bmBad *roaring.Bitmap
	for i, row := range rows {
		if _, err = stmt.Exec((*row)[idxBegin:idxEnd]...); err != nil {
			if bmBad == nil {
				errExec = errors.Wrapf(err, "stmt.Exec")
				bmBad = roaring.NewBitmap()
			}
			bmBad.AddInt(i)
		}
	}
	if errExec != nil {
		stmt.Close()
		_ = tx.Rollback()
		numBad = int(bmBad.GetCardinality())
		logger.Warn(fmt.Sprintf("writeRows skipped %d rows of %d due to invalid content", numBad, len(rows)), zap.Error(errExec))
		if tx, err = conn.Begin(); err != nil {
			err = errors.Wrapf(err, "conn.Begin %s", prepareSQL)
			return
		}
		if stmt, err = tx.Prepare(prepareSQL); err != nil {
			err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
			_ = tx.Rollback()
			return
		}
		defer stmt.Close()
		for i, row := range rows {
			if !bmBad.ContainsInt(i) {
				if _, err = stmt.Exec((*row)[idxBegin:idxEnd]...); err != nil {
					err = errors.Wrapf(err, "stmt.Exec")
					break
				}
			}
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if err = tx.Commit(); err != nil {
			err = errors.Wrapf(err, "tx.commit")
			return
		}
		return
	}
	if err = tx.Commit(); err != nil {
		err = errors.Wrapf(err, "tx.commit")
		return
	}
	return
}
