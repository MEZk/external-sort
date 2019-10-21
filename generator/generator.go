package generator

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
)

const (
	defaultDictionary = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	writeErr          = "cannot write randomly generated string: %s"
)

// Generator is a base interface for data generation.
type Generator interface {

	// Generate generates random data.
	Generate() error
}

// randomStringGenerator implements Generator interface to generate random string data
// which can be write to internal io.Writer.
type randomStringGenerator struct {
	linesNumber int
	lineLength  int
	linesDelim  rune
	dictionary  string
	out         io.Writer
}

// New returns randomStringGenerator which writes specified number of randomly generated strings
// to underlining io.Writer.
// Predefined dictionary is used to generate a random string. By default dictionary contains digits, lowercase,
// and uppercase English letters.
// Each string has specified number of characters.
// The total number of generated strings is set by linesNumber parameter.
// Each line length is set by lineLength parameter.
// Lines delimiter is set by delimiter parameter.
func New(out io.Writer, dictionary string, linesNumber, linesLength int, delimiter rune) *randomStringGenerator {
	var d string
	if dictionary == "" {
		d = defaultDictionary
	} else {
		d = dictionary
	}

	return &randomStringGenerator{
		out:         out,
		dictionary:  d,
		linesNumber: linesNumber,
		lineLength:  linesLength,
		linesDelim:  delimiter,
	}
}

// Generate generates random lines, writes them to the internal io.Writer, and flushes io.Writer.
// Each line is represented as a string and ends with the new line character.
// The last line always contains the new line character.
func (g *randomStringGenerator) Generate() error {
	w := bufio.NewWriter(g.out)
	defer w.Flush()

	for i := 0; i < g.linesNumber; i++ {

		_, err := w.Write(generateRandomString(g.dictionary, g.lineLength))
		if err != nil {
			return fmt.Errorf(writeErr, err)
		}

		_, err = w.WriteRune(g.linesDelim)
		if err != nil {
			return fmt.Errorf(writeErr, err)
		}
	}

	return nil
}

func generateRandomString(dictionary string, length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = dictionary[rand.Int63()%int64(len(dictionary))]
	}
	return b
}
