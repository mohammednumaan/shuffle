package mapper

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
)

type testMapper struct{}

func (tm *testMapper) Map(key string, value string, emit types.Emitter) {
	for _, word := range strings.Fields(value) {
		if err := emit.Emit(word, "1"); err != nil {
			return
		}
	}
}

func newTestConfig(t *testing.T, data string) *config.Config {
	t.Helper()

	inputFile := filepath.Join(t.TempDir(), "input.txt")
	if err := os.WriteFile(inputFile, []byte(data), 0o644); err != nil {
		t.Fatalf("writing test input failed: %v", err)
	}

	cfg := &config.Config{
		InputFile:   inputFile,
		OutputDir:   filepath.Join(t.TempDir(), "output"),
		StartOffset: 0,
		EndOffset:   int64(len(data)),
		NumReducers: 3,
	}
	cfg.RegisterFn(&testMapper{})

	return cfg
}

func TestProcessSplitReadsWholeFile(t *testing.T) {
	cfg := newTestConfig(t, "apple kiwi\nmango kiwi\n")

	intermediateData, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

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

	intermediateData, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

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

	intermediateData, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	if _, exists := intermediateData["epsilon"]; exists {
		t.Fatal("expected records starting at or after the end offset to be excluded")
	}
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		if _, exists := intermediateData[key]; !exists {
			t.Fatalf("expected key %q to exist", key)
		}
	}
}

func TestProcessFileSplitReadsLastSplitWithoutTrailingNewline(t *testing.T) {
	data := "alpha beta\ngamma delta\nepsilon zeta"
	cfg := newTestConfig(t, data)
	cfg.StartOffset = int64(strings.Index(data, "epsilon"))
	cfg.EndOffset = int64(len(data))

	intermediateData, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

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

	intermediateData, err := processSplit(cfg)
	if err != nil {
		t.Fatalf("process split returned an error: %v", err)
	}

	expectedByPartition := map[int][]string{}
	keys := make([]string, 0, len(intermediateData))
	for key := range intermediateData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		partition := getPartition(key, cfg.NumReducers)
		for _, value := range intermediateData[key] {
			expectedByPartition[partition] = append(expectedByPartition[partition], key+","+value)
		}
	}

	for partition := 0; partition < cfg.NumReducers; partition++ {
		partitionPath := filepath.Join(cfg.OutputDir, fmt.Sprintf("partition-%d", partition))
		data, err := os.ReadFile(partitionPath)
		if err != nil {
			t.Fatalf("reading partition file %s failed: %v", partitionPath, err)
		}

		content := strings.TrimSpace(string(data))
		var got []string
		if content != "" {
			got = strings.Split(content, "\n")
		}

		expected := expectedByPartition[partition]
		if len(got) != len(expected) {
			t.Fatalf("expected %d lines in partition %d, got %d", len(expected), partition, len(got))
		}
		for i := range expected {
			if got[i] != expected[i] {
				t.Fatalf("expected line %d in partition %d to be %q, got %q", i, partition, expected[i], got[i])
			}
		}
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
