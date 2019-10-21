package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/mezk/external-sort/generator"
	"io"
	"os"

	"github.com/mezk/external-sort/extsort"
)

const (
	mb             = 10 ^ 6
	memLimit       = 10 * mb
	lineLength     = 30
	linesNumber    = 1000000
	linesDelim     = '\n'
	largeFileName  = "largeFile.txt"
	outputFileName = "sorted_" + largeFileName
)

func main() {
	generateLargeFile()
	sortLargeFile()
}

func generateLargeFile() {
	fmt.Printf("Start large file generation: %s\n", largeFileName)

	outputFile, fileCreationErr := os.OpenFile(largeFileName, os.O_CREATE|os.O_WRONLY, 0644)
	if fileCreationErr != nil {
		panic(fmt.Errorf("cannot create %s: %s", largeFileName, fileCreationErr))
	}
	defer outputFile.Close()

	generationErr := generator.New(outputFile, "", linesNumber, lineLength, linesDelim).Generate()
	if generationErr != nil {
		panic(fmt.Errorf("cannot generate %s: %s", largeFileName, fileCreationErr))
	}

	fmt.Printf("Finish large file generation: %s\n", largeFileName)
}

func sortLargeFile() {
	fmt.Println("Sort engine initialization ...")

	chunkFunc := func(r io.Reader) ([]byte, error) {
		reader := bufio.NewReader(r)
		return reader.ReadBytes(byte(linesDelim))
	}

	lessFunc := func(b1 []byte, b2 []byte) bool {
		return bytes.Compare(b1, b2) < 0
	}

	sortedFile, createOutputFileErr := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY, 0644)
	if createOutputFileErr != nil {
		panic(fmt.Errorf("cannot create output file %s: %s", outputFileName, createOutputFileErr))
	}
	defer sortedFile.Close()

	engine, createEngineErr := extsort.New(sortedFile, chunkFunc, lessFunc, memLimit)
	if createEngineErr != nil {
		panic(fmt.Errorf("cannot create sort engine: %s", createEngineErr))
	}
	defer func() {
		fmt.Println("Sorting ...")
		// Sort on close
		engine.Close()
		fmt.Printf("Sort was finished. Output file: %s\n", outputFileName)
	}()

	fmt.Println("Sort engine is initialized successfully")

	inputFile, openInputFileErr := os.OpenFile(largeFileName, os.O_RDONLY, 0644)
	if openInputFileErr != nil {
		panic(fmt.Errorf("cannot open file %s: %s", largeFileName, openInputFileErr))
	}
	defer inputFile.Close()

	reader := bufio.NewReader(inputFile)
	for {
		line, readErr := reader.ReadBytes(byte(linesDelim))
		_, writeErr := engine.Write(line)
		if writeErr != nil {
			panic(fmt.Errorf("cannot write to %s: %s", outputFileName, writeErr))
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				panic(fmt.Errorf("cannot read from %s: %s", largeFileName, readErr))
			}
		}
	}
}
