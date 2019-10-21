package fileheap

// FileHeap implements heap.Interface interface.
// It stores fileheap.Entry entries.
// The lessFunc function is used in order to compare entries in the heap.
type FileHeap struct {
	entries  []*Entry
	lessFunc func([]byte, []byte) bool
}

// Entry represents a single heap Entry.
type Entry struct {
	// Temp file index to search for file descriptor
	FileIdx int

	// Current line read from file represented as array of bytes
	Data []byte
}

// NewHeap returns a new file heap. The lessFunc function serves to compare entries in the heap.
func NewHeap(lessFunc func([]byte, []byte) bool) *FileHeap {
	return &FileHeap{lessFunc: lessFunc}
}

// NewEntry returns a new heap entry.
func NewEntry(fileIdx int, data []byte) *Entry {
	return &Entry{FileIdx: fileIdx, Data: data}
}

// Len returns the number of elements in the heap.
func (fh *FileHeap) Len() int {
	return len(fh.entries)
}

// Swap swaps the elements with indexes i and j.
func (fh *FileHeap) Swap(i, j int) {
	fh.entries[i], fh.entries[j] = fh.entries[j], fh.entries[i]
}

// Less reports whether the element with index i should placed before the element with index j.
func (fh *FileHeap) Less(i, j int) bool {
	return fh.lessFunc(fh.entries[i].Data, fh.entries[j].Data)
}

// Push pushes the element x onto the heap.
func (fh *FileHeap) Push(x interface{}) {
	fh.entries = append(fh.entries, x.(*Entry))
}

// Pop removes and returns the minimum element (according to Less) from the heap.
func (fh *FileHeap) Pop() interface{} {
	n := len(fh.entries)
	e := fh.entries[n-1]
	fh.entries = fh.entries[:n-1]
	return e
}
