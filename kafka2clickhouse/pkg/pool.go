package pkg

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

var (
	lock        sync.Mutex
	clusterConn []*shardConn
	dsnSuffix   string
)

type shardConn struct {
	lock        sync.Mutex
	db          *sql.DB
	dbVer       int
	dsn         string
	replicas    []string
	maxOpenConn int
	nextRep     int
}

func (sc *shardConn) getDsn() string {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc.dsn
}

func (sc *shardConn) nextGoodReplica(failedVer int) (db *sql.DB, dbVer int, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.db != nil {
		if sc.dbVer > failedVer {
			return sc.db, sc.dbVer, nil
		}
		sc.db.Close()
		sc.db = nil
	}
	savedNextRep := sc.nextRep
	for i := 0; i < len(sc.replicas); i++ {
		sc.dsn = fmt.Sprintf("tcp://%s", sc.replicas[sc.nextRep]) + dsnSuffix
		sc.nextRep = (sc.nextRep + 1) % len(sc.replicas)
		sqlDB, err := sql.Open("clickhouse", sc.dsn)
		if err != nil {
			logger.Warn("sql.Open failed", zap.String("dsn", sc.dsn), zap.Error(err))
			continue
		}
		if err := sqlDB.Ping(); err != nil {
			logger.Warn("sqlDB.Ping failed", zap.String("dsn", sc.dsn), zap.Error(err))
			continue
		}

		sqlDB.SetMaxOpenConns(sc.maxOpenConn)
		sqlDB.SetMaxIdleConns(0)
		sqlDB.SetConnMaxIdleTime(10 * time.Second)
		sc.db = sqlDB
		sc.dbVer++
		logger.Info("sql.Open and sqlDB.Ping succeeded", zap.Int("dbVer", sc.dbVer), zap.String("dsn", sc.dsn))
		return sc.db, sc.dbVer, nil
	}
	err = errors.Errorf("no good replica among replicas %v since %d", sc.replicas, savedNextRep)
	return nil, sc.dbVer, err
}

func getShardConn(batchNum int64) (sc *shardConn) {
	lock.Lock()
	defer lock.Unlock()
	sc = clusterConn[batchNum%int64(len(clusterConn))]
	return
}
