package mapper

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/utils"
)

func Run(cfg *config.Config) error {
	if err := utils.ValidateMapperConfig(cfg); err != nil {
		return fmt.Errorf("mapper config: %w", err)
	}

	if _, err := processSplit(cfg); err != nil {
		return err
	}

	return nil
}

func processSplit(cfg *config.Config) ([]types.KeyValue[string, string], error) {
	if err := utils.ValidateMapperConfig(cfg); err != nil {
		return nil, err
	}

	kvs, err := processFileSplit(cfg)
	if err != nil {
		return nil, err
	}

	if err := writePartitions(cfg, kvs, cfg.OutputDir); err != nil {
		return nil, err
	}

	return kvs, nil
}

func processFileSplit(cfg *config.Config) ([]types.KeyValue[string, string], error) {
	file, err := os.Open(cfg.InputFile)
	if err != nil {
		return nil, fmt.Errorf("opening input file: %w", err)
	}

	defer file.Close()

	if _, err := file.Seek(cfg.StartOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to offset %d: %w", cfg.StartOffset, err)
	}

	reader := bufio.NewReader(file)
	currentOffset := cfg.StartOffset

	if cfg.StartOffset > 0 {
		previousByte := make([]byte, 1)
		if _, err := file.ReadAt(previousByte, cfg.StartOffset-1); err != nil {
			return nil, fmt.Errorf("reading byte before start offset: %w", err)
		}

		if previousByte[0] != '\n' {
			discarded, err := reader.ReadString('\n')
			currentOffset += int64(len(discarded))

			if err != nil {
				if err == io.EOF {
					return nil, nil
				}
				return nil, fmt.Errorf("skipping partial line: %w", err)
			}
		}
	}

	var kvs []types.KeyValue[string, string]
	for {
		recordStart := currentOffset
		line, err := reader.ReadString('\n')
		currentOffset += int64(len(line))

		line = strings.TrimSuffix(line, "\n")
		if line != "" && recordStart < cfg.EndOffset {
			kvs = append(kvs, cfg.Mapper.Map(strconv.FormatInt(recordStart, 10), line)...)
		}

		if err == io.EOF {
			return kvs, nil
		}

		if err != nil {
			return nil, fmt.Errorf("reading line: %w", err)
		}

		if currentOffset >= cfg.EndOffset {
			return kvs, nil
		}
	}
}

func writePartitions(cfg *config.Config, kvs []types.KeyValue[string, string], outputDir string) error {
	numPartitions := cfg.NumReducers
	if numPartitions <= 0 {
		return errors.New("num reducers must be greater than zero")
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("output dir %s: %w", outputDir, err)
	}

	files := make([]*os.File, numPartitions)
	writers := make([]*bufio.Writer, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionName := fmt.Sprintf("partition-%d", i)
		fileName := filepath.Join(outputDir, partitionName)

		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", fileName, err)
		}

		files[i] = file
		writers[i] = bufio.NewWriter(file)
	}

	for _, kv := range kvs {
		p, err := getPartition(kv.Key, numPartitions)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(writers[p]).Encode(kv); err != nil {
			return fmt.Errorf("encoding key %q: %w", kv.Key, err)
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

func getPartition(key string, numPartitions int) (int, error) {
	if numPartitions <= 0 {
		return 0, errors.New("num partitions must be greater than zero")
	}

	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, fmt.Errorf("calculating hash: %w", err)
	}

	hashValue := h.Sum32()
	return int(hashValue % uint32(numPartitions)), nil
}
