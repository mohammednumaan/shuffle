package reducer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
)

func Run(cfg *config.Config) error {
	if err := validateReducerConfig(cfg); err != nil {
		return fmt.Errorf("reducer config: %w", err)
	}

	groupedData, err := collectPartitionData(cfg.InputDir, cfg.ReducerIdx)
	if err != nil {
		return fmt.Errorf("collecting partition data: %w", err)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("output dir %s: %w", cfg.OutputDir, err)
	}

	if err := writeReducerOutput(cfg, groupedData); err != nil {
		return err
	}

	return nil
}

func validateReducerConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}

	if cfg.InputDir == "" {
		return errors.New("input dir is required")
	}

	if cfg.OutputDir == "" {
		return errors.New("output dir is required")
	}

	if cfg.NumReducers <= 0 {
		return errors.New("number of reducers must be greater than 0")
	}

	if cfg.ReducerIdx < 0 || cfg.ReducerIdx >= cfg.NumReducers {
		return errors.New("invalid reducer index")
	}

	if cfg.Reducer == nil {
		return errors.New("reducer is nil")
	}
	return nil
}

func collectPartitionData(inputDir string, reducerIdx int) (map[string][]string, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("reading input directory failed: %w", err)
	}

	partitionName := fmt.Sprintf("partition-%d", reducerIdx)
	grouped := make(map[string][]string)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		partitionFilePath := filepath.Join(inputDir, entry.Name(), partitionName)
		if err := readPartitionFile(partitionFilePath, grouped); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("reading partition file %s failed: %w", partitionFilePath, err)
		}
	}

	return grouped, nil
}

func readPartitionFile(filePath string, grouped map[string][]string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening partition file failed: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(bufio.NewReader(file))
	for {
		var record types.KeyValue[string, string]
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decoding partition file failed: %w", err)
		}
		grouped[record.Key] = append(grouped[record.Key], record.Value)
	}
}

func writeReducerOutput(cfg *config.Config, groupedData map[string][]string) error {
	outputPath := filepath.Join(cfg.OutputDir, fmt.Sprintf("reducer-%d", cfg.ReducerIdx))
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output file failed: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)

	keys := make([]string, 0, len(groupedData))
	for key := range groupedData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		values := groupedData[key]
		res, err := cfg.Reducer.Reduce(key, values)

		if err != nil {
			return fmt.Errorf("reducing key %s failed: %w", key, err)
		}

		if err := encoder.Encode(types.KeyValue[string, string]{Key: key, Value: res}); err != nil {
			return fmt.Errorf("encoding reducer output failed: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flushing reducer output failed: %w", err)
	}

	return nil
}
