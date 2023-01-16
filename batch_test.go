/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func TestWriteBatch(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}

	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		defer wb.Cancel()

		N, M := 50000, 1000
		start := time.Now()

		for i := 0; i < N; i++ {
			require.NoError(t, wb.Set(key(i), val(i)))
		}
		for i := 0; i < M; i++ {
			require.NoError(t, wb.Delete(key(i)))
		}
		require.NoError(t, wb.Flush())
		t.Logf("Time taken for %d writes (w/ test options): %s\n", N+M, time.Since(start))

		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			defer itr.Close()

			i := M
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, string(key(i)), string(item.Key()))
				valcopy, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, val(i), valcopy)
				i++
			}
			require.Equal(t, N, i)
			return nil
		})
		require.NoError(t, err)
	})
}

// This test ensures we don't panic during flush.
// See issue: https://github.com/dgraph-io/badger/issues/1394
func TestFlushPanic(t *testing.T) {
	t.Run("flush after flush", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			wb := db.NewWriteBatch()
			wb.Flush()
			require.Error(t, y.ErrCommitAfterFinish, wb.Flush())
		})
	})
	t.Run("flush after cancel", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			wb := db.NewWriteBatch()
			wb.Cancel()
			require.Error(t, y.ErrCommitAfterFinish, wb.Flush())
		})
	})
}

func TestBatchErrDeadlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	db, err := OpenManaged(opt)
	require.NoError(t, err)

	wb := db.NewManagedWriteBatch()
	require.NoError(t, wb.SetEntry(&Entry{Key: []byte("foo")}))
	require.Error(t, wb.Flush())
	require.NoError(t, db.Close())
}
