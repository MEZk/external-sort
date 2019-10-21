package extsort

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/mezk/external-sort/fileheap"
)

const (
	memoryBufferFlushErr = "cannot flush memory buffer to temp file: %s"
	defaultIoBufferSize  = 65536
)

// Chunk is a function that chunks data read from the given io.Reader into items for sorting.
type Chunk func(io.Reader) ([]byte, error)

// Less is a function that compares two byte arrays (a1 and a2) and determines whether a2 is less than a2.
type Less func(a1 []byte, a2 []byte) bool

// sortEngine implements io.WriteCloser which sorts its output on writing.
// Each []byte passed to the Write function is considered as a single item to sort.
type sortEngine struct {
	memLimit       int
	memUsed        int
	tmpDir         string
	chunkFunc      Chunk
	lessFunc       Less
	out            io.Writer
	tmpFilesNumber int
	memoryBuffer   [][]byte
}

// New returns a new io.WriteCloser that wraps out, chunks data into sortable
// items using the given chunkFunc function, compares them using the given lessFunc function, and limits
// the amount of memory (RAM) used to approximately memLimit.
func New(out io.Writer, chunkFunc Chunk, lessFunc Less, memLimit int) (io.WriteCloser, error) {
	tmpDir, tmpDirCreationErr := ioutil.TempDir("", "extsort_tmp_files")
	if tmpDirCreationErr != nil {
		return nil, fmt.Errorf("cannot create working directory to store temp files: %s", tmpDirCreationErr)
	}

	return &sortEngine{
		memLimit:  memLimit,
		tmpDir:    tmpDir,
		chunkFunc: chunkFunc,
		lessFunc:  lessFunc,
		out:       out,
	}, nil
}

// Write writes bytes in memory buffer or flushes memory buffer to temp file if memory limit exceeded.
func (se *sortEngine) Write(b []byte) (int, error) {
	se.memoryBuffer = append(se.memoryBuffer, b)
	se.memUsed += len(b)

	if se.memUsed >= se.memLimit {
		flushErr := se.flushToTempFile()
		if flushErr != nil {
			return 0, fmt.Errorf(memoryBufferFlushErr, flushErr)
		}
	}

	return len(b), nil
}

// Close closes io.WriteCloser and performs K-way merge of temp files
// which represent sorted segments of the original large file.
// It also cleans the directory with temp files and frees memory buffers.
func (se *sortEngine) Close() error {
	defer se.removeTempFiles()

	if se.memUsed > 0 {
		// Memory buffer is not empty on close. Flush is required.
		err := se.flushToTempFile()
		if err != nil {
			return fmt.Errorf(memoryBufferFlushErr, err)
		}
	}

	// Free memory buffer
	se.memoryBuffer = nil

	files := make(map[int]*bufio.Reader, se.tmpFilesNumber)
	for i := 0; i < se.tmpFilesNumber; i++ {
		file, openTmpFileErr := os.OpenFile(filepath.Join(se.tmpDir, strconv.Itoa(i)), os.O_RDONLY, 0)
		if openTmpFileErr != nil {
			return fmt.Errorf("cannot open temp file: %s\n", openTmpFileErr)
		}
		defer file.Close()
		files[i] = bufio.NewReaderSize(file, defaultIoBufferSize)
	}

	if se.tmpFilesNumber == 1 {
		// There is only one temp file with sorted data
		_, copyErr := io.Copy(se.out, files[0])
		if copyErr != nil {
			return fmt.Errorf("cannot write temp file content to output: %s\n", copyErr)
		}
	} else {
		sortErr := se.sort(files)
		if sortErr != nil {
			return fmt.Errorf("cannot sort temp files: %s\n", sortErr)
		}
	}

	switch c := se.out.(type) {
	case io.Closer:
		return c.Close()
	default:
		// Closing of output is not supported
		return nil
	}
}

func (se *sortEngine) flushToTempFile() error {
	// Sort memory buffer content using lessFunc function
	sort.Sort(&fileChunkInMemoryRepresentation{se.memoryBuffer, se.lessFunc})

	file, tmpFileCreationErr := os.OpenFile(
		filepath.Join(se.tmpDir, strconv.Itoa(se.tmpFilesNumber)),
		os.O_CREATE|os.O_WRONLY, 0644)
	if tmpFileCreationErr != nil {
		return fmt.Errorf("cannot create temp file to flush memory buffer: %s\n", tmpFileCreationErr)
	}
	defer file.Close()

	out := bufio.NewWriterSize(file, defaultIoBufferSize)
	for _, bytes := range se.memoryBuffer {
		_, writeMemoryBufferErr := out.Write(bytes)
		if writeMemoryBufferErr != nil {
			return fmt.Errorf("cannot write to temp file: %s\n", writeMemoryBufferErr)
		}
	}

	flushToTempFileErr := out.Flush()
	if flushToTempFileErr != nil {
		return fmt.Errorf("cannot flush to temp file: %s\n", flushToTempFileErr)
	}

	se.tmpFilesNumber++
	se.memUsed = 0

	// Reallocate memory buffer
	se.memoryBuffer = make([][]byte, 0, len(se.memoryBuffer))

	return nil
}

func (se *sortEngine) sort(files map[int]*bufio.Reader) error {
	defer se.removeTempFiles()

	fileHeap := fileheap.NewHeap(se.lessFunc)

	memLimitPerFile := se.memLimit / (se.tmpFilesNumber + 1)

	fillHeap := func() error {
		for i := 0; i < len(files); i++ {
			file := files[i]
			readBytes := 0
			for {
				b, chunkFileErr := se.chunkFunc(file)
				if chunkFileErr == io.EOF {
					delete(files, i)
					break
				}
				if chunkFileErr != nil {
					return fmt.Errorf("cannot chunk file: %s\n", chunkFileErr)
				}
				readBytes += len(b)
				heap.Push(fileHeap, fileheap.NewEntry(i, b))
				if readBytes >= memLimitPerFile {
					break
				}
			}
		}

		return nil
	}

	for {
		if fileHeap.Len() == 0 {
			fillHeapErr := fillHeap()
			if fillHeapErr != nil {
				return fmt.Errorf("cannot fill heap: %s\n", fillHeapErr)
			}
		}
		if fileHeap.Len() == 0 {
			// Nothing left to sort
			break
		}

		heapEntry := heap.Pop(fileHeap).(*fileheap.Entry)
		_, writeErr := se.out.Write(heapEntry.Data)
		if writeErr != nil {
			return fmt.Errorf("cannot write heap entry data to result output: %s\n", writeErr)
		}

		// Read next chunk of data from heap top file
		file := files[heapEntry.FileIdx]
		if file != nil {
			b, err := se.chunkFunc(file)
			if err == io.EOF {
				delete(files, heapEntry.FileIdx)
				continue
			}
			if err != nil {
				return fmt.Errorf("error replacing entry on heap: %s\n", err)
			}
			heap.Push(fileHeap, fileheap.NewEntry(heapEntry.FileIdx, b))
		}
	}

	return nil
}

func (se *sortEngine) removeTempFiles() {
	fmt.Printf("Removing temp files in %s\n", se.tmpDir)
	err := os.RemoveAll(se.tmpDir)
	if err != nil {
		panic(fmt.Errorf("cannot remove temp files in %s: %s\n", se.tmpDir, err))
	}
	fmt.Println("Successfully finished temp files removal process")
}

type fileChunkInMemoryRepresentation struct {
	memoryBuffer [][]byte
	lessFunc     func(a []byte, b []byte) bool
}

func (fc *fileChunkInMemoryRepresentation) Len() int {
	return len(fc.memoryBuffer)
}

func (fc *fileChunkInMemoryRepresentation) Less(i, j int) bool {
	return fc.lessFunc(fc.memoryBuffer[i], fc.memoryBuffer[j])
}

func (fc *fileChunkInMemoryRepresentation) Swap(i, j int) {
	fc.memoryBuffer[i], fc.memoryBuffer[j] = fc.memoryBuffer[j], fc.memoryBuffer[i]
}
