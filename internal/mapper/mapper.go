package mapper

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
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
	if err := validateMapperConfig(cfg); err != nil {
		log.Fatalf("invalid mapper config: %v", err)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		log.Fatalf("creating directory %s failed: %v", cfg.OutputDir, err)
	}

	if _, err := processSplit(cfg); err != nil {
		log.Fatalf("mapper failed: %v", err)
	}
}

func validateMapperConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}

	if cfg.InputFile == "" {
		return errors.New("input file is required")
	}

	if cfg.OutputDir == "" {
		return errors.New("output dir is required")
	}

	if cfg.EndOffset < cfg.StartOffset {
		return errors.New("end offset must be greater than or equal to start offset")
	}

	if cfg.Mapper == nil {
		return errors.New("mapper is nil")
	}

	return nil
}

func processSplit(cfg *config.Config) (map[string][]string, error) {
	if err := validateMapperConfig(cfg); err != nil {
		return nil, err
	}

	intermediatePairs := make(map[string][]string)
	emit := &Emitter{intermediatePairs: intermediatePairs}

	if err := processFileSplit(cfg, emit); err != nil {
		return nil, err
	}

	if err := writeIntermediatePairsToDisk(cfg, intermediatePairs, cfg.OutputDir); err != nil {
		return nil, err
	}

	return intermediatePairs, nil
}

func processFileSplit(cfg *config.Config, emit types.Emitter) error {
	file, err := os.Open(cfg.InputFile)
	if err != nil {
		return fmt.Errorf("opening input file: %w", err)
	}

	defer file.Close()

	if _, err := file.Seek(cfg.StartOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to offset %d: %w", cfg.StartOffset, err)
	}

	reader := bufio.NewReader(file)
	currentOffset := cfg.StartOffset

	if cfg.StartOffset > 0 {
		previousByte := make([]byte, 1)
		if _, err := file.ReadAt(previousByte, cfg.StartOffset-1); err != nil {
			return fmt.Errorf("reading byte before start offset: %w", err)
		}

		if previousByte[0] != '\n' {
			discarded, err := reader.ReadString('\n')
			currentOffset += int64(len(discarded))

			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("skipping partial line: %w", err)
			}

			if errors.Is(err, io.EOF) {
				return nil
			}
		}
	}

	for {
		recordStart := currentOffset
		line, err := reader.ReadString('\n')
		currentOffset += int64(len(line))

		line = strings.TrimSuffix(line, "\n")
		if line != "" && recordStart < cfg.EndOffset {
			cfg.Mapper.Map(strconv.FormatInt(recordStart, 10), line, emit)
		}

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("reading line: %w", err)
		}

		if currentOffset >= cfg.EndOffset {
			return nil
		}
	}
}

func writeIntermediatePairsToDisk(cfg *config.Config, intermediatePairs map[string][]string, outputDir string) error {
	numPartitions := cfg.NumReducers
	if numPartitions <= 0 {
		return errors.New("num reducers must be greater than zero")
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir %s: %w", outputDir, err)
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
			return fmt.Errorf("opening file %s: %w", fileName, err)
		}

		files = append(files, file)
		writers = append(writers, bufio.NewWriter(file))
	}

	for _, key := range keys {
		p := getPartition(key, numPartitions)
		if err := writeToFile(writers[p], key, intermediatePairs[key]); err != nil {
			return err
		}
	}

	for _, writer := range writers {
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("flushing writer: %w", err)
		}
	}

	for _, file := range files {
		if err := file.Close(); err != nil {
			return fmt.Errorf("closing file: %w", err)
		}
	}

	return nil
}

func writeToFile(writer *bufio.Writer, key string, values []string) error {
	encoder := json.NewEncoder(writer)
	for _, value := range values {
		record := types.IntermediateRecord{
			Key:   key,
			Value: value,
		}
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("encoding key %q: %w", key, err)
		}
	}
	return nil
}

func getPartition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		log.Fatalf("num partitions must be greater than zero")
	}

	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		log.Fatalf("calculating hash failed: %v", err)
	}

	hashValue := h.Sum32()
	return int(hashValue % uint32(numPartitions))
}
