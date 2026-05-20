package mapper

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
)

type Emitter struct {
	intermediatePairs map[string][]string
}

func (em *Emitter) Emit(key, value string) error {
	if em == nil {
		return errors.New("emitter is nil")
	}
	if em.intermediatePairs == nil {
		return errors.New("intermediate pairs map is nil")
	}
	em.intermediatePairs[key] = append(em.intermediatePairs[key], value)
	return nil
}

func Run(cfg *config.Config) {
	processFiles(cfg)
}

func processFiles(cfg *config.Config) (map[string][]string, error) {
	inputDir := cfg.InputDir
	startAndEndFiles := strings.Split(cfg.FilePartition, ",")
	if len(startAndEndFiles) != 2 {
		fmt.Printf("Invalid file partition %q: expected format startFile,endFile\n", cfg.FilePartition)
		return nil, errors.New("invalid file partition format")
	}

	startFile := startAndEndFiles[0]
	endFile := startAndEndFiles[1]

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		fmt.Printf("Error reading input directory: %v\n", err)
		return nil, err
	}

	intermediatePairs := make(map[string][]string)
	emit := &Emitter{
		intermediatePairs: intermediatePairs,
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()

		if filename >= startFile && filename <= endFile {
			filePath := filepath.Join(inputDir, filename)
			processFile(cfg, filePath, emit)
		}
	}

	return intermediatePairs, nil

}

func processFile(cfg *config.Config, filePath string, emit types.Emitter) {
	if cfg.Mapper == nil {
		fmt.Printf("No mapper registered for file %s\n", filePath)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", filePath, err)
		return
	}
	defer file.Close()

	// the mapper should do something like this:
	// Map(k1, v1) -> list(k2, v2)
	// where k1 is some input key and v1 is the corresponding value.
	// the user defined map function will take k1 and v1 as input and produce a list of intermediate key-value pairs (k2, v2) which i can add to my in-memory list

	scanner := bufio.NewScanner(file)
	lineNumber := 0

	for scanner.Scan() {
		line := scanner.Text()
		cfg.Mapper.Map(strconv.Itoa(lineNumber), line, emit)
		lineNumber++
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error scanning file %s: %v\n", filePath, err)
	}
}
