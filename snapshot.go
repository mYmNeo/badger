package badger

import (
	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

type SnapshotLevels struct {
	levels []*levelHandler
}

func (s *SnapshotLevels) get(key []byte) (y.ValueStruct, error) {
	var maxVs y.ValueStruct
	return s.getFromLevel(key, &maxVs)
}

func (s *SnapshotLevels) getFromLevel(key []byte, maxVs *y.ValueStruct) (y.ValueStruct, error) {
	// It's important that we iterate the levels from 0 on upward.  The reason is, if we iterated
	// in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
	// read level L's tables post-compaction and level L+1's tables pre-compaction.  (If we do
	// parallelize this, we will need to call the h.RLock() function by increasing order of level
	// number.)
	version := y.ParseTs(key)
	for _, h := range s.levels {
		vs, err := h.get(key) // Calls h.RLock() and h.RUnlock().
		if err != nil {
			return y.ValueStruct{}, errors.Wrapf(err, "get key: %q", key)
		}
		if vs.Value == nil && vs.Meta == 0 {
			continue
		}
		if maxVs == nil || vs.Version == version {
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			*maxVs = vs
		}
	}
	if maxVs != nil {
		return *maxVs, nil
	}
	return y.ValueStruct{}, nil
}

func (s *SnapshotLevels) Close() {
	for _, h := range s.levels {
		for _, t := range h.tables {
			t.DecrRef()
		}
	}
}
