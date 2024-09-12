package table

type EmptyTable struct {
	tableSize int64
}

var _ TableInterface = (*EmptyTable)(nil)

func NewEmptyTable(tableSize int64) *EmptyTable {
	return &EmptyTable{
		tableSize: tableSize,
	}
}

func (e *EmptyTable) Smallest() []byte {
	return nil
}

func (e *EmptyTable) Biggest() []byte {
	return nil
}

func (e *EmptyTable) DoesNotHave(key []byte) bool {
	return false
}

func (e *EmptyTable) Size() int64 {
	return e.tableSize
}
