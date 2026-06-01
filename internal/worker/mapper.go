package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/validation"
)

var mapperFn types.Mapper = defaultMapper{}
var reducerFn types.Reducer = defaultReducer{}

func RegisterFunctions(mapper types.Mapper, reducer types.Reducer) error {
	if err := validation.ValidateMapper(mapper); err != nil {
		return err
	}
	if err := validation.ValidateReducer(reducer); err != nil {
		return err
	}

	mapperFn = mapper
	reducerFn = reducer
	return nil
}

type defaultMapper struct{}

func (defaultMapper) Map(_ string, value string) []types.KeyValue[string, string] {
	var kvs []types.KeyValue[string, string]
	for _, word := range strings.Fields(value) {
		kvs = append(kvs, types.KeyValue[string, string]{Key: word, Value: "1"})
	}
	return kvs
}

type defaultReducer struct{}

func (defaultReducer) Reduce(_ string, values []string) (string, error) {
	return strconv.Itoa(len(values)), nil
}

func executeMapTask(task *types.Task, workerAddress string) ([]types.PartitionLocationInfo, error) {
	if err := validation.ValidateMapTask(task); err != nil {
		return nil, err
	}

	outputDir := filepath.Join(os.TempDir(), "shuffle", task.JobId, task.TaskId)

	log.Printf("[Mapper %s] processing file=%s offset=%d-%d", task.TaskId, task.Split.FilePath, task.Split.StartOffset, task.Split.EndOffset)

	kvs, err := processFileSplit(task.Split, mapperFn)
	if err != nil {
		return nil, fmt.Errorf("process file split: %w", err)
	}

	log.Printf("[Mapper %s] produced %d key-value pairs", task.TaskId, len(kvs))

	if err := writePartitions(task.NumReducers, kvs, outputDir); err != nil {
		return nil, fmt.Errorf("write partitions: %w", err)
	}

	log.Printf("[Mapper %s] wrote %d partitions to %s", task.TaskId, task.NumReducers, outputDir)

	locations := make([]types.PartitionLocationInfo, task.NumReducers)
	for i := 0; i < task.NumReducers; i++ {
		partitionName := fmt.Sprintf("partition-%d", i)
		partitionPath := filepath.Join(outputDir, partitionName)
		locations[i] = types.PartitionLocationInfo{
			FilePath:      partitionPath,
			PartitionIdx:  i,
			WorkerAddress: workerAddress,
		}
	}
	return locations, nil
}

func processFileSplit(split *types.InputSplit, mapper types.Mapper) ([]types.KeyValue[string, string], error) {
	if err := validation.ValidateMapper(mapper); err != nil {
		return nil, err
	}
	if err := validation.ValidateInputSplit(split); err != nil {
		return nil, err
	}

	file, err := os.Open(split.FilePath)
	if err != nil {
		return nil, fmt.Errorf("opening input file: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(split.StartOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to offset %d: %w", split.StartOffset, err)
	}

	reader := bufio.NewReader(file)
	currentOffset := split.StartOffset

	if split.StartOffset > 0 {
		previousByte := make([]byte, 1)
		if _, err := file.ReadAt(previousByte, split.StartOffset-1); err != nil {
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
		if line != "" && recordStart < split.EndOffset {
			kvs = append(kvs, mapper.Map(strconv.FormatInt(recordStart, 10), line)...)
		}

		if err == io.EOF {
			return kvs, nil
		}

		if err != nil {
			return nil, fmt.Errorf("reading line: %w", err)
		}

		if currentOffset >= split.EndOffset {
			return kvs, nil
		}
	}
}

func writePartitions(numPartitions int, kvs []types.KeyValue[string, string], outputDir string) error {
	if err := validation.ValidateNumPartitions(numPartitions); err != nil {
		return err
	}
	if err := validation.ValidateOutputDir(outputDir); err != nil {
		return err
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("output dir %s: %w", outputDir, err)
	}

	files := make([]*os.File, numPartitions)
	writers := make([]*bufio.Writer, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionName := fmt.Sprintf("partition-%d", i)
		fileName := filepath.Join(outputDir, partitionName)

		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", fileName, err)
		}

		files[i] = file
		writers[i] = bufio.NewWriter(file)
	}

	for _, kv := range kvs {
		partition, err := getPartition(kv.Key, numPartitions)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(writers[partition]).Encode(kv); err != nil {
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
	if err := validation.ValidateNumPartitions(numPartitions); err != nil {
		return 0, err
	}

	h := fnv.New32a()
	if _, err := h.Write([]byte(key)); err != nil {
		return 0, fmt.Errorf("calculating hash: %w", err)
	}

	hashValue := h.Sum32()
	return int(hashValue % uint32(numPartitions)), nil
}
