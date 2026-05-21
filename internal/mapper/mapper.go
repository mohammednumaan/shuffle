package mapper

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
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
	// first i need to make sure
	// the output directory is created to storet the key-value pairs
	if err := os.MkdirAll(cfg.OutputDir, 0777); err != nil {
		log.Fatalf("Creating directory %s failed: %v", cfg.OutputDir, err)
	}
	// then i can safely process files
	processFiles(cfg)
}

func processFiles(cfg *config.Config) (map[string][]string, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}

	inputDir := cfg.InputDir
	startAndEndFiles := strings.Split(cfg.FilePartition, ",")

	if len(startAndEndFiles) != 2 {
		fmt.Printf("Invalid file partition %q: expected format startFile,endFile\n", cfg.FilePartition)
		return nil, errors.New("invalid file partition format")
	}

	// todo: add proper validation for this
	startFile := startAndEndFiles[0]
	endFile := startAndEndFiles[1]

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		fmt.Printf("Error reading input directory: %v\n", err)
		return nil, err
	}

	// this is the in-memory buffer the paper talks about
	// the intermediate key-value pairs emitted by the user-defined map fn will
	// be stored in this "buffer"
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

	// after the key-avlue pairs are generated, i need to write the to disk
	// its partitioned into R files containing the data we need
	if err := writeIntermediatePairsToDisk(cfg, intermediatePairs, cfg.OutputDir); err != nil {
		return nil, err
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

func writeIntermediatePairsToDisk(cfg *config.Config, intermediatePairs map[string][]string, outputDir string) error {
	// the output path is in this format
	// nfs_path/jobid/mapperId
	// according to the paper, i need to partition this into R "regions" or files
	// so i need to define a "partition function"
	numPartitions := cfg.NumReducers
	if numPartitions <= 0 {
		return errors.New("num reducers must be greater than zero")
	}
	if err := os.MkdirAll(outputDir, 0777); err != nil {
		return fmt.Errorf("failed to create output dir %s: %w", outputDir, err)
	}

	keys := make([]string, 0, len(intermediatePairs))
	for key := range intermediatePairs {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	writers := make([]*bufio.Writer, 0, numPartitions)
	files := make([]*os.File, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionName := fmt.Sprintf("partition-%d", i)
		fileName := filepath.Join(outputDir, partitionName)

		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", fileName, err)
		}

		files = append(files, file)
		writer := bufio.NewWriter(file)
		writers = append(writers, writer)
	}

	for _, key := range keys {
		p := getPartition(key, numPartitions)
		if err := writeToFile(writers[p], key, intermediatePairs[key]); err != nil {
			return err
		}
	}

	for i, writer := range writers {
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush partition-%d: %w", i, err)
		}
	}
	for i, file := range files {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close partition-%d: %w", i, err)
		}
	}

	return nil
}

func writeToFile(writer *bufio.Writer, key string, values []string) error {
	for _, value := range values {
		_, err := writer.WriteString(fmt.Sprintf("%s,%s\n", key, value))
		if err != nil {
			return fmt.Errorf("failed to write key %q: %w", key, err)
		}
	}
	return nil
}

func getPartition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		log.Fatalf("numPartitions must be greater than zero")
	}
	// this is a simple mod-based hash genration
	// hFn(key) -> hash(key) % bounds (in this case numPartitions)
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		log.Fatalf("Error calculating hash: %v", err)
	}
	hashValue := h.Sum32()
	return int(hashValue % uint32(numPartitions))

}
