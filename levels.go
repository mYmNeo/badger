/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

type levelsController struct {
	nextFileID uint64 // Atomic
	// The following are initialized once and const.
	levels []*levelHandler
	kv     *DB

	cstatus compactStatus
}

var (
	// This is for getting timings between stalls.
	lastUnstalled time.Time
)

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest. idMap is a set of table file id's that were read from the directory
// listing.
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.Tables[id]; !ok {
			filename := table.NewFilename(id, kv.opt.Dir)
			if err := os.Remove(filename); err != nil {
				return y.Wrapf(err, "While removing table %d", id)
			}
		}
	}

	return nil
}

func newLevelsController(db *DB, mf *Manifest) (*levelsController, error) {
	y.AssertTrue(db.opt.NumLevelZeroTablesStall > db.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:     db,
		levels: make([]*levelHandler, db.opt.MaxLevels),
	}
	s.cstatus.levels = make([]*levelCompactStatus, db.opt.MaxLevels)

	for i := 0; i < db.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(db, i)
		if i == 0 {
			// Do nothing.
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = db.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * int64(db.opt.LevelSizeMultiplier)
		}
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	// Compare manifest against directory, check for existent/non-existent files, and remove.
	if err := revertToManifest(db, mf, getIDMap(db.opt.Dir)); err != nil {
		return nil, err
	}

	// Some files may be deleted. Let's reload.
	var flags uint32 = y.Sync
	if db.opt.ReadOnly {
		flags |= y.ReadOnly
	}

	var mu sync.Mutex
	tables := make([][]*table.Table, db.opt.MaxLevels)
	var maxFileID uint64

	// We found that using 3 goroutines allows disk throughput to be utilized to its max.
	// Disk utilization is the main thing we should focus on, while trying to read the data. That's
	// the one factor that remains constant between HDD and SSD.
	throttle := y.NewThrottle(6)

	start := time.Now()
	var numOpened int32
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	for fileID, tf := range mf.Tables {
		fname := table.NewFilename(fileID, db.opt.Dir)
		select {
		case <-tick.C:
			db.opt.Infof("%d tables out of %d opened in %s\n", atomic.LoadInt32(&numOpened),
				len(mf.Tables), time.Since(start).Round(time.Millisecond))
		default:
		}
		if err := throttle.Do(); err != nil {
			closeAllTables(tables)
			return nil, err
		}
		if fileID > maxFileID {
			maxFileID = fileID
		}
		go func(fname string, tf TableManifest) {
			var rerr error
			defer func() {
				throttle.Done(rerr)
				atomic.AddInt32(&numOpened, 1)
			}()
			fd, err := y.OpenExistingFile(fname, flags)
			if err != nil {
				rerr = errors.Wrapf(err, "Opening file: %q", fname)
				return
			}

			t, err := table.OpenTable(fd, db.opt.TableLoadingMode, tf.Checksum)
			if err != nil {
				if strings.HasPrefix(err.Error(), "CHECKSUM_MISMATCH:") {
					db.opt.Errorf(err.Error())
					db.opt.Errorf("Ignoring table %s", fd.Name())
					// Do not set rerr. We will continue without this table.
				} else {
					rerr = errors.Wrapf(err, "Opening table: %q", fname)
				}
				return
			}

			mu.Lock()
			tables[tf.Level] = append(tables[tf.Level], t)
			mu.Unlock()
		}(fname, tf)
	}
	if err := throttle.Finish(); err != nil {
		closeAllTables(tables)
		return nil, err
	}
	db.opt.Infof("All %d tables opened in %s\n", atomic.LoadInt32(&numOpened),
		time.Since(start).Round(time.Millisecond))
	s.nextFileID = maxFileID + 1
	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}

	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, errors.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	if err := syncDir(db.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]*table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close()
		}
	}
}

func (s *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range s.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dropTree picks all tables from all levels, creates a manifest changeset,
// applies it, and then decrements the refs of these tables, which would result
// in their deletion.
func (s *levelsController) dropTree() (int, error) {
	// First pick all tables, so we can create a manifest changelog.
	var all []*table.Table
	for _, l := range s.levels {
		l.RLock()
		all = append(all, l.tables...)
		l.RUnlock()
	}
	if len(all) == 0 {
		return 0, nil
	}

	// Generate the manifest changes.
	changes := []*pb.ManifestChange{}
	for _, table := range all {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	changeSet := pb.ManifestChangeSet{Changes: changes}
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return 0, err
	}

	// Now that manifest has been successfully written, we can delete the tables.
	for _, l := range s.levels {
		l.Lock()
		l.totalSize = 0
		l.tables = l.tables[:0]
		l.Unlock()
	}
	for _, table := range all {
		if err := table.DecrRef(); err != nil {
			return 0, err
		}
	}
	return len(all), nil
}

// dropPrefix runs a L0->L1 compaction, and then runs same level compaction on the rest of the
// levels. For L0->L1 compaction, it runs compactions normally, but skips over all the keys with the
// provided prefix and also the internal move keys for the same prefix.
// For Li->Li compactions, it picks up the tables which would have the prefix. The
// tables who only have keys with this prefix are quickly dropped. The ones which have other keys
// are run through MergeIterator and compacted to create new tables. All the mechanisms of
// compactions apply, i.e. level sizes and MANIFEST are updated as in the normal flow.
func (s *levelsController) dropPrefixes(prefixes [][]byte) error {
	opt := s.kv.opt
	// Iterate levels in the reverse order because if we were to iterate from
	// lower level (say level 0) to a higher level (say level 3) we could have
	// a state in which level 0 is compacted and an older version of a key exists in lower level.
	// At this point, if someone creates an iterator, they would see an old
	// value for a key from lower levels. Iterating in reverse order ensures we
	// drop the oldest data first so that lookups never return stale data.
	for i := len(s.levels) - 1; i >= 0; i-- {
		l := s.levels[i]

		l.RLock()
		if l.level == 0 {
			size := len(l.tables)
			l.RUnlock()

			if size > 0 {
				cp := &compactionPriority{
					level: 0,
					score: 1.74,
					// A unique number greater than 1.0 does two things. Helps identify this
					// function in logs, and forces a compaction.
					dropPrefixes: prefixes,
				}
				if err := s.doCompact(174, cp); err != nil {
					opt.Warningf("While compacting level 0: %v", err)
					return nil
				}
			}
			continue
		}

		// Build a list of compaction tableGroups affecting all the prefixes we
		// need to drop. We need to build tableGroups that satisfy the invariant that
		// bottom tables are consecutive.
		// tableGroup contains groups of consecutive tables.
		var tableGroups [][]*table.Table
		var tableGroup []*table.Table

		finishGroup := func() {
			if len(tableGroup) > 0 {
				tableGroups = append(tableGroups, tableGroup)
				tableGroup = nil
			}
		}

		for _, tb := range l.tables {
			if containsAnyPrefixes(tb, prefixes) {
				tableGroup = append(tableGroup, tb)
			} else {
				finishGroup()
			}
		}
		finishGroup()

		l.RUnlock()

		if len(tableGroups) == 0 {
			continue
		}

		opt.Infof("Dropping prefix at level %d (%d tableGroups)", l.level, len(tableGroups))
		for _, operation := range tableGroups {
			cd := &compactDef{
				thisLevel:    l,
				nextLevel:    l,
				top:          nil,
				bot:          operation,
				dropPrefixes: prefixes,
				nextRange:    getKeyRange(operation...),
			}
			if err := s.runCompactDef(l.level, cd, false); err != nil {
				opt.Warningf("While running compact def: %+v. Error: %v", cd, err)
				return err
			}
		}
	}
	return nil
}

func (s *levelsController) startCompact(lc *y.Closer) {
	n := s.kv.opt.NumCompactors
	lc.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		// The worker with id=0 is dedicated to L0 and L1. This is not counted
		// towards the user specified NumCompactors.
		go s.runCompactor(i, lc)
	}
}

func (s *levelsController) runCompactor(id int, lc *y.Closer) {
	defer lc.Done()

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lc.HasBeenClosed():
		randomDelay.Stop()
		return
	}

	moveL0toFront := func(prios []*compactionPriority) []*compactionPriority {
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]*compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}

	run := func(p *compactionPriority) bool {
		s.kv.opt.Debugf("Compaction priority: %+v", p)
		err := s.doCompact(id, p)
		switch err {
		case nil:
			return true
		case errFillTables:
			// pass
		default:
			s.kv.opt.Warningf("While running doCompact: %v\n", err)
		}
		return false
	}

	var priosBuffer []*compactionPriority
	runOnce := func() bool {
		prios := s.pickCompactLevels(priosBuffer)
		defer func() {
			priosBuffer = prios
		}()
		if id == 0 {
			// Worker ID zero prefers to compact L0 always.
			prios = moveL0toFront(prios)
		}
		for _, p := range prios {
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			if run(p) {
				return true
			}
		}

		return false
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			runOnce()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// Returns true if level zero may be compacted, without accounting for compactions that already
// might be happening.
func (s *levelsController) isLevel0Compactable() bool {
	return s.levels[0].numTables() >= s.kv.opt.NumLevelZeroTables
}

// Returns true if the non-zero level may be compacted.  delSize provides the size of the tables
// which are currently being compacted so that we treat them as already having started being
// compacted (because they have been, yet their size is already counted in getTotalSize).
func (l *levelHandler) isCompactable(delSize int64) bool {
	return l.getTotalSize()-delSize >= l.maxTotalSize
}

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	// dryRun is set to true when we want to simulate a compaction. In this case, we don't actually
	// build the tables, but just calculate the deletedTableSize and addedTableSize.
	dryRun bool

	botSize    int64
	newBotSize int64
}

// pickCompactLevel determines which level to compact.
// Based on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
func (s *levelsController) pickCompactLevels(priosBuffer []*compactionPriority) (prios []*compactionPriority) {
	// This function must use identical criteria for guaranteeing compaction's progress that
	// addLevel0Table uses.

	addPriority := func(level int, score float64) {
		pri := &compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
		}
		prios = append(prios, pri)
	}

	// Grow buffer to fit all levels.
	if cap(priosBuffer) < len(s.levels) {
		priosBuffer = make([]*compactionPriority, 0, len(s.levels))
	}
	prios = priosBuffer[:0]

	addPriority(0, float64(s.levels[0].numTables())/float64(s.kv.opt.NumLevelZeroTables))
	for i := 1; i < len(s.levels); i++ {
		// Don't consider those tables that are already being compacted right now.
		delSize := s.cstatus.delSize(i)
		l := s.levels[i]
		sz := l.getTotalSize() - delSize
		addPriority(i, float64(sz)/float64(l.maxTotalSize))
	}
	y.AssertTrue(len(prios) == len(s.levels))

	// The following code is borrowed from PebbleDB and results in healthier LSM tree structure.
	// If Li-1 has score > 1.0, then we'll divide Li-1 score by Li. If Li score is >= 1.0, then Li-1
	// score is reduced, which means we'll prioritize the compaction of lower levels (L5, L4 and so
	// on) over the higher levels (L0, L1 and so on). On the other hand, if Li score is < 1.0, then
	// we'll increase the priority of Li-1.
	// Overall what this means is, if the bottom level is already overflowing, then de-prioritize
	// compaction of the above level. If the bottom level is not full, then increase the priority of
	// above level.
	var prevLevel int
	for level := 0; level < len(s.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// Pick all the levels whose original score is >= 1.0, irrespective of their adjusted score.
	// We'll still sort them by their adjusted score below. Having both these scores allows us to
	// make better decisions about compacting L0. If we see a score >= 1.0, we can do L0->L0
	// compactions. If the adjusted score >= 1.0, then we can do L0->Lbase compactions.
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out
	// Sort by the adjusted score.
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// checkOverlap checks if the given tables overlap with any level from the given "lev" onwards.
func (s *levelsController) checkOverlap(tables []*table.Table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range s.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// compactBuildTables merges topTables and botTables to form a list of new tables.
func (s *levelsController) compactBuildTables(lev int, cd *compactDef, dryRun bool) (interface{}, func() error, error) {
	topTables := cd.top
	botTables := cd.bot

	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	hasOverlap := s.checkOverlap(cd.allTables(), cd.nextLevel.level+1)

	// Create iterators across all the tables involved first.
	var iters []y.Iterator
	if lev == 0 {
		iters = appendIteratorsReversed(iters, topTables, false)
	} else {
		iters = []y.Iterator{table.NewConcatIterator(topTables, false)}
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	var valid []*table.Table

nextTable:
	for _, table := range botTables {
		if len(cd.dropPrefixes) > 0 {
			for _, prefix := range cd.dropPrefixes {
				if bytes.HasPrefix(table.Smallest(), prefix) &&
					bytes.HasPrefix(table.Biggest(), prefix) {
					// All the keys in this table have the dropPrefix. So, this
					// table does not need to be in the iterator and can be
					// dropped immediately.
					continue nextTable
				}
			}
		}
		valid = append(valid, table)
	}
	iters = append(iters, table.NewConcatIterator(valid, false))
	it := table.NewMergeIterator(iters, false)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.Rewind()

	var filter CompactionFilter
	if s.kv.opt.CompactionFilterFactory != nil {
		filter = s.kv.opt.CompactionFilterFactory()
	}

	// Pick a discard ts, so we can discard versions below this ts. We should
	// never discard any versions starting from above this timestamp, because
	// that would affect the snapshot view guarantee provided by transactions.
	discardTs := s.kv.orc.discardAtOrBelow()

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) {
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	var newTi []table.TableInterface
	mu := new(sync.Mutex) // Guards newTables
	maxConcurrentBuild := 10
	if dryRun {
		maxConcurrentBuild = 1000
	}
	inflightBuilders := y.NewThrottle(maxConcurrentBuild)

	var numBuilds, numVersions int
	var lastKey, skipKey []byte
	tableSize := s.kv.opt.MaxTableSize
	for it.Valid() {
		timeStart := time.Now()
		builder := table.NewTableBuilder(tableSize)
		var numKeys, numSkips uint64
		for ; it.Valid(); it.Next() {
			// See if we need to skip the prefix.
			if len(cd.dropPrefixes) > 0 && hasAnyPrefixes(it.Key(), cd.dropPrefixes) {
				updateStats(it.Value())
				numSkips++
				continue
			}

			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if y.SameKey(it.Key(), skipKey) {
					updateStats(it.Value())
					numSkips++
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			if !y.SameKey(it.Key(), lastKey) {
				if builder.ReachedCapacity() {
					// Only break if we are on a different key, and have reached capacity. We want
					// to ensure that all versions of the key are stored in the same sstable, and
					// not divided across multiple tables at the same level.
					break
				}
				lastKey = y.SafeCopy(lastKey, it.Key())
				numVersions = 0
			}

			vs := it.Value()
			version := y.ParseTs(it.Key())
			// Do not discard entries inserted by merge operator. These entries will be
			// discarded once they're merged
			if version <= discardTs && vs.Meta&bitMergeEntry == 0 {
				// Keep track of the number of versions encountered for this key. Only consider the
				// versions which are below the minReadTs, otherwise, we might end up discarding the
				// only valid version for a running transaction.
				numVersions++

				// Keep the current version and discard all the next versions if
				// - The `discardEarlierVersions` bit is set OR
				// - We've already processed `NumVersionsToKeep` number of versions
				// (including the current item being processed)
				lastValidVersion := vs.Meta&bitDiscardEarlierVersions > 0 ||
					numVersions == s.kv.opt.NumVersionsToKeep

				isExpired := isDeletedOrExpired(vs.Meta, vs.ExpiresAt)

				if isExpired || lastValidVersion {
					// If this version of the key is deleted or expired, skip all the rest of the
					// versions. Ensure that we're only removing versions below readTs.
					skipKey = y.SafeCopy(skipKey, it.Key())

					switch {
					// Add the key to the table only if it has not expired.
					// We don't want to add the deleted/expired keys.
					case !isExpired && lastValidVersion:
						// Add this key. We have set skipKey, so the following key versions
						// would be skipped.
					case hasOverlap:
						// If this key range has overlap with lower levels, then keep the deletion
						// marker with the latest version, discarding the rest. We have set skipKey,
						// so the following key versions would be skipped.
					default:
						// If no overlap, we can skip all the versions, by continuing here.
						numSkips++
						updateStats(vs)
						continue // Skip adding this key.
					}
				}
			}
			if filter != nil {
				switch filter.Filter(it.Key(), vs.Value, vs.UserMeta, lev) {
				case DecisionDelete:
					// Convert to delete tombstone.
					builder.Add(it.Key(), y.ValueStruct{Meta: bitDelete})
					continue
				case DecisionDrop:
					updateStats(vs)
					continue
				case DecisionKeep:
				}
			}
			numKeys++
			builder.Add(it.Key(), vs)
		}
		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		s.kv.opt.Debugf("LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			numKeys, numSkips, time.Since(timeStart))
		if builder.Empty() {
			// Cleanup builder resources:
			builder.Finish()
			builder.Close()
			continue
		}
		numBuilds++
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}

		build := func(builder *table.Builder) (table.TableInterface, error) {
			fileID := s.reserveFileID()
			fd, err := y.CreateSyncedFile(table.NewFilename(fileID, s.kv.opt.Dir), true)
			if err != nil {
				return nil, errors.Wrapf(err, "While opening new table: %d", fileID)
			}

			if _, err := fd.Write(builder.Finish()); err != nil {
				return nil, errors.Wrapf(err, "Unable to write to file: %d", fileID)
			}
			newTbl, err := table.OpenTable(fd, s.kv.opt.TableLoadingMode, nil)
			// decrRef is added below.
			return newTbl, errors.Wrapf(err, "Unable to open table: %q", fd.Name())
		}

		if dryRun {
			build = func(builder *table.Builder) (table.TableInterface, error) {
				return table.NewEmptyTable(int64(len(builder.Finish()))), nil
			}
		}

		go func(builder *table.Builder) {
			var (
				err error
				tbl table.TableInterface
			)

			defer builder.Close()
			defer inflightBuilders.Done(err)

			tbl, err = build(builder)
			// If we couldn't build the table, return fast.
			if err != nil {
				return
			}

			mu.Lock()
			newTi = append(newTi, tbl)
			mu.Unlock()
		}(builder)
	}

	// Wait for all table builders to finish and also for newTables accumulator to finish.
	err := inflightBuilders.Finish()
	if err == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		err = syncDir(s.kv.opt.Dir)
	}

	if dryRun {
		return newTi, func() error { return nil }, err
	}

	s.kv.vlog.updateDiscardStats(discardStats)
	s.kv.opt.Debugf("Discard stats: %v", discardStats)

	newTables := make([]*table.Table, len(newTi))
	for i, t := range newTi {
		newTables[i] = t.(*table.Table)
	}

	if err != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		_ = decrRefs(newTables)
		return nil, nil, errors.Wrapf(err, "while running compactions for: %+v", cd)
	}
	sortTables(newTables)
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func buildChangeSet(cd *compactDef, newTables []*table.Table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.ID(), cd.nextLevel.level, table.Checksum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func hasAnyPrefixes(s []byte, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if bytes.HasPrefix(s, prefix) {
			return true
		}
	}

	return false
}

func containsPrefix(table *table.Table, prefix []byte) bool {
	smallValue := table.Smallest()
	largeValue := table.Biggest()
	if bytes.HasPrefix(smallValue, prefix) {
		return true
	}
	if bytes.HasPrefix(largeValue, prefix) {
		return true
	}
	isPresent := func() bool {
		ti := table.NewIterator(false)
		defer ti.Close()
		// In table iterator's Seek, we assume that key has version in last 8 bytes. We set
		// version=0 (ts=math.MaxUint64), so that we don't skip the key prefixed with prefix.
		ti.Seek(y.KeyWithTs(prefix, math.MaxUint64))
		return bytes.HasPrefix(ti.Key(), prefix)
	}
	if bytes.Compare(prefix, smallValue) > 0 &&
		bytes.Compare(prefix, largeValue) < 0 {
		// There may be a case when table contains [0x0000,...., 0xffff]. If we are searching for
		// k=0x0011, we should not directly infer that k is present. It may not be present.
		return isPresent()
	}

	return false
}

func containsAnyPrefixes(table *table.Table, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if containsPrefix(table, prefix) {
			return true
		}
	}

	return false
}

type compactDef struct {
	thisLevel *levelHandler
	nextLevel *levelHandler

	top []*table.Table
	bot []*table.Table

	thisRange keyRange
	nextRange keyRange

	topSize     int64
	topLeftIdx  int
	topRightIdx int
	botSize     int64
	botLeftIdx  int
	botRightIdx int

	dropPrefixes [][]byte
	newSize      int64
}

func (cd *compactDef) String() string {
	return fmt.Sprintf("%d top:[%d:%d](%d), bot:[%d:%d](%d), write_amp:%.2f",
		cd.thisLevel.level, cd.topLeftIdx, cd.topRightIdx, cd.topSize,
		cd.botLeftIdx, cd.botRightIdx, cd.botSize, float64(cd.topSize+cd.botSize)/float64(cd.topSize))
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	if cd.thisLevel.level != cd.nextLevel.level {
		cd.nextLevel.RLock()
	}
}

func (cd *compactDef) unlockLevels() {
	if cd.thisLevel.level != cd.nextLevel.level {
		cd.nextLevel.RUnlock()
	}
	cd.thisLevel.RUnlock()
}

func (cd *compactDef) allTables() []*table.Table {
	ret := make([]*table.Table, 0, len(cd.top)+len(cd.bot))
	ret = append(ret, cd.top...)
	ret = append(ret, cd.bot...)
	return ret
}

func (s *levelsController) fillTablesL0(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if len(cd.thisLevel.tables) == 0 {
		return false
	}

	cd.top = make([]*table.Table, 0, len(cd.thisLevel.tables))
	for _, t := range cd.thisLevel.tables {
		cd.topSize += t.Size()
		cd.top = append(cd.top, t)
		// not pick all tables in L0, pick some tables in L0 which sum of sizes is less than maxCompactionExpandSize
		if cd.topSize >= maxCompactionExpandSize {
			break
		}
	}
	cd.topRightIdx = len(cd.top)
	cd.thisRange = infRange

	kr := getKeyRange(cd.top...)
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, kr)
	overlappingTables := cd.nextLevel.tables[left:right]
	cd.botLeftIdx = left
	cd.botRightIdx = right
	cd.bot = make([]*table.Table, len(overlappingTables))
	copy(cd.bot, overlappingTables)
	for _, t := range cd.bot {
		cd.botSize += t.Size()
	}

	if len(overlappingTables) == 0 { // the bottom-most level
		cd.nextRange = kr
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}

	if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
		return false
	}

	return true
}

// sortByOverlap sorts tables in increasing order of overlap with next level.
func (s *levelsController) sortByOverlap(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	tableOverlap := make([]int, len(tables))
	for i := range tables {
		// get key range for table
		tableRange := getKeyRange(tables[i])
		// get overlap with next level
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, tableRange)
		tableOverlap[i] = right - left
	}

	sort.Slice(tables, func(i, j int) bool {
		return tableOverlap[i] < tableOverlap[j]
	})
}

const maxCompactionExpandSize = 1 << 30 // 1GB

func (s *levelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if len(cd.thisLevel.tables) == 0 {
		return false
	}
	this := make([]*table.Table, len(cd.thisLevel.tables))
	copy(this, cd.thisLevel.tables)
	next := make([]*table.Table, len(cd.nextLevel.tables))
	copy(next, cd.nextLevel.tables)

	// First pick one table has max topSize/bottomSize ratio.
	var candidateRatio float64
	for i, t := range this {
		if s.isCompacting(cd.thisLevel.level, t) {
			continue
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if s.isCompacting(cd.nextLevel.level, next[left:right]...) {
			continue
		}
		botSize := sumTableSize(next[left:right])
		ratio := calcRatio(t.Size(), botSize)
		if ratio > candidateRatio {
			candidateRatio = ratio
			cd.topLeftIdx = i
			cd.topRightIdx = i + 1
			cd.top = this[cd.topLeftIdx:cd.topRightIdx:cd.topRightIdx]
			cd.topSize = t.Size()
			cd.botLeftIdx = left
			cd.botRightIdx = right
			cd.botSize = botSize
		}
	}
	if len(cd.top) == 0 {
		return false
	}
	bots := next[cd.botLeftIdx:cd.botRightIdx:cd.botRightIdx]
	// Expand to left to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topLeftIdx - 1; i >= 0; i-- {
		t := this[i]
		if s.isCompacting(cd.thisLevel.level, t) {
			break
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if right < cd.botLeftIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		if s.isCompacting(cd.nextLevel.level, next[left:cd.botLeftIdx]...) {
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[left:cd.botLeftIdx]) + cd.botSize
		newRatio := calcRatio(newTopSize, newBotSize)
		if newRatio > candidateRatio && (newTopSize+newBotSize) < maxCompactionExpandSize {
			cd.top = append([]*table.Table{t}, cd.top...)
			cd.topLeftIdx--
			bots = append(next[left:cd.botLeftIdx:cd.botLeftIdx], bots...)
			cd.botLeftIdx = left
			cd.topSize = newTopSize
			cd.botSize = newBotSize
		} else {
			break
		}
	}
	// Expand to right to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topRightIdx; i < len(this); i++ {
		t := this[i]
		if s.isCompacting(cd.thisLevel.level, t) {
			break
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if left > cd.botRightIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		if s.isCompacting(cd.nextLevel.level, next[cd.botRightIdx:right]...) {
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[cd.botRightIdx:right]) + cd.botSize
		newRatio := calcRatio(newTopSize, newBotSize)
		if newRatio > candidateRatio && (newTopSize+newBotSize) < maxCompactionExpandSize {
			cd.top = append(cd.top, t)
			cd.topRightIdx++
			bots = append(bots, next[cd.botRightIdx:right]...)
			cd.botRightIdx = right
			cd.topSize = newTopSize
			cd.botSize = newBotSize
		} else {
			break
		}
	}
	cd.thisRange = keyRange{left: cd.top[0].Smallest(), right: cd.top[len(cd.top)-1].Biggest()}
	if len(bots) > 0 {
		cd.nextRange = keyRange{left: bots[0].Smallest(), right: bots[len(bots)-1].Biggest()}
	} else {
		cd.nextRange = cd.thisRange
	}
	cd.bot = make([]*table.Table, len(bots))
	copy(cd.bot, bots)
	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func sumTableSize(tables []*table.Table) int64 {
	var size int64
	for _, t := range tables {
		size += t.Size()
	}
	return size
}

func calcRatio(topSize, botSize int64) float64 {
	if botSize == 0 {
		return float64(topSize)
	}
	return float64(topSize) / float64(botSize)
}

func (s *levelsController) isCompacting(level int, tables ...*table.Table) bool {
	if len(tables) == 0 {
		return false
	}
	kr := keyRange{
		left:  tables[0].Smallest(),
		right: tables[len(tables)-1].Biggest(),
	}
	y.AssertTrue(len(kr.left) != 0)
	y.AssertTrue(len(kr.right) != 0)
	return s.cstatus.overlapsWith(level, kr)
}

func (s *levelsController) runCompactDef(l int, cd *compactDef, dryRun bool) (err error) {
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	// Table should never be moved directly between levels, always be rewritten to allow discarding
	// invalid versions.

	timeStart := time.Now()
	newData, decr, err := s.compactBuildTables(l, cd, dryRun)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()

	s.kv.opt.Infof("LOG Compact %d->%d, compactBuildTables took %v", thisLevel.level, nextLevel.level, time.Since(timeStart))
	if dryRun {
		cd.newSize = 0
		for _, t := range newData.([]table.TableInterface) {
			cd.newSize += t.Size()
		}
		return nil
	}

	y.NumCompactions.Add(1)
	newTables := newData.([]*table.Table)
	changeSet := buildChangeSet(cd, newTables)

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	if err := nextLevel.replaceTables(cd.bot, newTables, cd); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	s.kv.opt.Infof("LOG Compact %d->%d, del %d tables, add %d tables, took %v",
		thisLevel.level, nextLevel.level, len(cd.top)+len(cd.bot),
		len(newTables), time.Since(timeStart))
	return nil
}

var errFillTables = errors.New("Unable to fill tables")

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(id int, p *compactionPriority) error {
	l := p.level
	y.AssertTrue(l+1 < s.kv.opt.MaxLevels) // Sanity check.

	cd := &compactDef{
		thisLevel:    s.levels[l],
		nextLevel:    s.levels[l+1],
		dropPrefixes: p.dropPrefixes,
	}

	s.kv.opt.Debugf("[Compactor: %d] Attempting to run compaction: %+v", id, p)

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		if !s.fillTablesL0(cd) {
			return errFillTables
		}
	} else {
		if !s.fillTables(cd) {
			return errFillTables
		}
	}
	defer s.cstatus.delete(cd) // Remove the ranges from compaction status.

	s.kv.opt.Infof("[Compactor: %d] Running compaction: %+v. def %s", id, p, cd.String())
	if err := s.runCompactDef(l, cd, p.dryRun); err != nil {
		// This compaction couldn't be done successfully.
		s.kv.opt.Warningf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	if p.dryRun {
		p.botSize = cd.botSize
		p.newBotSize = cd.newSize
	}

	s.kv.opt.Infof("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.level)
	return nil
}

func (s *levelsController) addLevel0Table(t *table.Table) error {
	// We update the manifest _before_ the table becomes part of a levelHandler, because at that
	// point it could get used in some compaction.  This ensures the manifest file gets updated in
	// the proper order. (That means this update happens before that of some compaction which
	// deletes the table.)
	err := s.kv.manifest.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID(), 0, t.Checksum),
	})
	if err != nil {
		return err
	}

	for !s.levels[0].tryAddLevel0Table(t) {
		// Stall. Make sure all levels are healthy before we unstall.
		s.kv.opt.Infof("STALLED STALLED STALLED: %v\n", time.Since(lastUnstalled))
		// Before we unstall, we need to make sure that level 0 and 1 are healthy. Otherwise, we
		// will very quickly fill up level 0 again and if the compaction strategy favors level 0,
		// then level 1 is going to super full.
		// Passing 0 for delSize to compactable means we're treating incomplete compactions as
		// not having finished -- we wait for them to finish.  Also, it's crucial this behavior
		// replicates pickCompactLevels' behavior in computing compactability in order to
		// guarantee progress.
		for s.isLevel0Compactable() || s.levels[1].isCompactable(0) {
			time.Sleep(10 * time.Millisecond)
		}
		lastUnstalled = time.Now()
	}

	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return errors.Wrap(err, "levelsController.Close")
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(key []byte, maxVs *y.ValueStruct) (y.ValueStruct, error) {
	if s.kv.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
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

func appendIteratorsReversed(out []y.Iterator, th []*table.Table, reversed bool) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(reversed))
	}
	return out
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(
	iters []y.Iterator, opt *IteratorOptions) []y.Iterator {
	// Just like with get, it's important we iterate the levels from 0 on upward, to avoid missing
	// data when there's a compaction.
	for _, level := range s.levels {
		iters = level.appendIterators(iters, opt)
	}
	return iters
}

// TableInfo represents the information about a table.
type TableInfo struct {
	ID       uint64
	Level    int
	Left     []byte
	Right    []byte
	KeyCount uint64 // Number of keys in the table
}

func (s *levelsController) getTableInfo(withKeysCount bool) (result []TableInfo) {
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			var count uint64
			if withKeysCount {
				it := t.NewIterator(false)
				for it.Rewind(); it.Valid(); it.Next() {
					count++
				}
				it.Close()
			}

			info := TableInfo{
				ID:       t.ID(),
				Level:    l.level,
				Left:     t.Smallest(),
				Right:    t.Biggest(),
				KeyCount: count,
			}
			result = append(result, info)
		}
		l.RUnlock()
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].ID < result[j].ID
	})
	return
}

type LevelInfo struct {
	Level     int
	NumTables int
	Size      int64
	MaxSize   int64
}

func (s *levelsController) getLevelInfo() []LevelInfo {
	result := make([]LevelInfo, len(s.levels))
	for i, l := range s.levels {
		l.RLock()
		result[i].Level = i
		result[i].Size = l.totalSize
		result[i].NumTables = len(l.tables)
		result[i].MaxSize = l.maxTotalSize
		l.RUnlock()
	}
	return result
}
