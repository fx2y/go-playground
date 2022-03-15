package pkg

import (
	"container/list"
	"sync"
	"sync/atomic"
)

var (
	rowsPool sync.Pool
	rowPool  sync.Pool
)

type row []interface{}
type rows []*row

type batch struct {
	rows     *rows
	batchIdx int64
	group    *batchGroup
}

type batchGroup struct {
	offsets   map[int]int64
	sys       *batchSys
	pendWrite int32
}

type batchSys struct {
	mux      sync.Mutex
	groups   list.List
	fnCommit func(partition int, offset int64) error
}

func (bs *batchSys) tryCommit() error {
	bs.mux.Lock()
	defer bs.mux.Unlock()
LOOP:
	for e := bs.groups.Front(); e != nil; {
		grp, _ := e.Value.(*batchGroup)
		if atomic.LoadInt32(&grp.pendWrite) != 0 {
			break LOOP
		}
		for j, off := range grp.offsets {
			if err := bs.fnCommit(j, off); err != nil {
				return err
			}
		}
		eNext := e.Next()
		bs.groups.Remove(e)
		e = eNext
	}
	return nil
}

func (b *batch) commit() error {
	for _, row := range *b.rows {
		putRow(row)
	}
	putRows(b.rows)
	b.rows = nil
	atomic.AddInt32(&b.group.pendWrite, -1)
	return b.group.sys.tryCommit()
}

func putRows(rs *rows) {
	*rs = (*rs)[:0]
	rowsPool.Put(rs)
}

func putRow(r *row) {
	*r = (*r)[:0]
	rowPool.Put(r)
}
