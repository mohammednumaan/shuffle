package mapper

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mohammednumaan/shuffle/internal/types"
)

type testMapper struct{}
type testReducer struct{}

func (tm *testMapper) Map(key string, value string) []types.KeyValue[string, string] {
	var kvs []types.KeyValue[string, string]
	for _, word := range strings.Fields(value) {
		kvs = append(kvs, types.KeyValue[string, string]{Key: word, Value: "1"})
	}
	return kvs
}

func (tr *testReducer) Reduce(key string, values []string) (string, error) {
	return fmt.Sprintf("%d", len(values)), nil
}

func decodePartitionRecords(t *testing.T, path string) []types.KeyValue[string, string] {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening partition file failed: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var records []types.KeyValue[string, string]
	for {
		var record types.KeyValue[string, string]
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decoding intermediate record failed: %v", err)
		}
		records = append(records, record)
	}

	return records
}

func newTestConfig(t *testing.T, data string) *types.Config {
	t.Helper()

	inputFile := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(inputFile, []byte(data), 0o644); err != nil {
		t.Fatalf("writing test input failed: %v", err)
	}

	cfg := &types.Config{
		InputFile:   inputFile,
		OutputDir:   filepath.Join(t.TempDir(), "output"),
		StartOffset: 0,
		EndOffset:   int64(len(data)),
		NumReducers: 3,
	}
	cfg.RegisterFn(&testMapper{}, &testReducer{})

	return cfg
}

func groupByKey(kvs []types.KeyValue[string, string]) map[string][]string {
	grouped := make(map[string][]string)
	for _, kv := range kvs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}
	return grouped
}

func TestProcessSplitReadsWholeFile(t *testing.T) {
	cfg := newTestConfig(t, "apple kiwi\nmango kiwi\n")

	kvs, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	intermediateData := groupByKey(kvs)
	expected := map[string][]string{
		"apple": {"1"},
		"kiwi":  {"1", "1"},
		"mango": {"1"},
	}

	for key, expectedValues := range expected {
		values, exists := intermediateData[key]
		if !exists {
			t.Fatalf("expected key %q to exist", key)
		}
		if len(values) != len(expectedValues) {
			t.Fatalf("expected %d values for key %q, got %d", len(expectedValues), key, len(values))
		}
	}
}

func TestProcessFileSplitSkipsPartialFirstLine(t *testing.T) {
	data := "alpha beta\ngamma delta\nepsilon zeta\n"
	cfg := newTestConfig(t, data)
	cfg.StartOffset = int64(strings.Index(data, "beta"))
	cfg.EndOffset = int64(len(data))

	kvs, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	intermediateData := groupByKey(kvs)
	if _, exists := intermediateData["alpha"]; exists {
		t.Fatal("expected first partial line to be skipped")
	}
	for _, key := range []string{"gamma", "delta", "epsilon", "zeta"} {
		if _, exists := intermediateData[key]; !exists {
			t.Fatalf("expected key %q to exist", key)
		}
	}
}

func TestProcessFileSplitStopsAfterCrossingEndOffset(t *testing.T) {
	data := "alpha beta\ngamma delta\nepsilon zeta\n"
	cfg := newTestConfig(t, data)
	cfg.EndOffset = int64(strings.Index(data, "epsilon"))

	kvs, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	intermediateData := groupByKey(kvs)
	if _, exists := intermediateData["epsilon"]; exists {
		t.Fatal("expected records starting at or after the end offset to be excluded")
	}
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		if _, exists := intermediateData[key]; !exists {
			t.Fatalf("expected key %q to exist", key)
		}
	}
}

func TestProcessFileSplitDoesNotSkipLineIfStartOffsetIsAtValidLine(t *testing.T) {
	data := "alpha beta\ngamma delta\nepsilon zeta"
	cfg := newTestConfig(t, data)
	cfg.StartOffset = int64(strings.Index(data, "epsilon"))
	cfg.EndOffset = int64(len(data))

	kvs, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	intermediateData := groupByKey(kvs)
	for _, key := range []string{"epsilon", "zeta"} {
		if _, exists := intermediateData[key]; !exists {
			t.Fatalf("expected key %q to exist", key)
		}
	}
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		if _, exists := intermediateData[key]; exists {
			t.Fatalf("expected key %q to be excluded from the last split", key)
		}
	}
}

func TestProcessSplitWritesIntermediateFilesToDisk(t *testing.T) {
	cfg := newTestConfig(t, "apple kiwi\nmango kiwi\n")

	kvs, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	expectedByPartition := map[int][]types.KeyValue[string, string]{}
	for _, kv := range kvs {
		partition, err := getPartition(kv.Key, cfg.NumReducers)
		if err != nil {
			t.Fatalf("getPartition failed: %v", err)
		}
		expectedByPartition[partition] = append(expectedByPartition[partition], kv)
	}

	for partition := 0; partition < cfg.NumReducers; partition++ {
		partitionPath := filepath.Join(cfg.OutputDir, fmt.Sprintf("partition-%d", partition))
		got := decodePartitionRecords(t, partitionPath)
		expected := expectedByPartition[partition]
		if len(got) != len(expected) {
			t.Fatalf("expected %d records in partition %d, got %d", len(expected), partition, len(got))
		}
		for i := range expected {
			if got[i] != expected[i] {
				t.Fatalf("expected record %d in partition %d to be %+v, got %+v", i, partition, expected[i], got[i])
			}
		}
	}
}

func TestWritePartitionsUsesJSONLines(t *testing.T) {
	outputDir := t.TempDir()
	cfg := &types.Config{NumReducers: 1}
	kvs := []types.KeyValue[string, string]{
		{Key: `alpha,"beta"`, Value: "line 1\nline 2"},
	}

	if err := writePartitions(cfg, kvs, outputDir); err != nil {
		t.Fatalf("writePartitions returned an error: %v", err)
	}

	records := decodePartitionRecords(t, filepath.Join(outputDir, "partition-0"))

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Key != `alpha,"beta"` {
		t.Fatalf("expected key %q, got %q", `alpha,"beta"`, records[0].Key)
	}
	if records[0].Value != "line 1\nline 2" {
		t.Fatalf("expected value %q, got %q", "line 1\nline 2", records[0].Value)
	}
}

func TestProcessSplitCreatesOutputDirWhenMissing(t *testing.T) {
	cfg := newTestConfig(t, "apple kiwi\n")
	cfg.OutputDir = filepath.Join(t.TempDir(), "nested", "mapper-output")

	if _, err := processSplit(cfg); err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	for partition := 0; partition < cfg.NumReducers; partition++ {
		partitionPath := filepath.Join(cfg.OutputDir, fmt.Sprintf("partition-%d", partition))
		if _, err := os.Stat(partitionPath); err != nil {
			t.Fatalf("expected partition file %s to exist: %v", partitionPath, err)
		}
	}
}

func TestProcessSplitRejectsInvalidReducerCount(t *testing.T) {
	cfg := newTestConfig(t, "apple kiwi\n")
	cfg.NumReducers = 0

	_, err := processSplit(cfg)
	if err == nil {
		t.Fatal("expected process split to fail when num reducers is zero")
	}
}
