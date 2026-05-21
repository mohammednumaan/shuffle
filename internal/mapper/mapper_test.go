package mapper

import (
	"fmt"
	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func NewTestConfig() *config.Config {
	return &config.Config{
		InputDir:      "./test_files",
		OutputDir:     "./output",
		FilePartition: "new_file1.txt,new_file5.txt",
		NumReducers:   3,
	}
}

type TestMapper struct{}

func (tm *TestMapper) Map(key string, value string, emit types.Emitter) {
	for _, word := range strings.Fields(value) {
		if err := emit.Emit(word, "1"); err != nil {
			return
		}
	}
}

func TestProcessFiles(t *testing.T) {
	cfg := NewTestConfig()
	cfg.OutputDir = t.TempDir()
	cfg.RegisterFn(&TestMapper{})
	intermediateData, err := processFiles(cfg)
	if err != nil {
		t.Fatalf("ProcessFiles returned an error: %v", err)
	}

	expected := map[string][]string{
		"apple":       {"1"},
		"berry":       {"1"},
		"dragonfruit": {"1"},
		"kiwi":        {"1", "1"},
		"lime":        {"1", "1"},
		"mango":       {"1", "1", "1"},
		"papaya":      {"1"},
	}

	for key, expectedValues := range expected {
		values, exists := intermediateData[key]
		if !exists {
			t.Errorf("Expected key %q not found in intermediate data", key)
			continue
		}
		if len(values) != len(expectedValues) {
			t.Errorf("For key %q, expected %d values but got %d", key, len(expectedValues), len(values))
			continue
		}

		for i, expectedValue := range expectedValues {
			if values[i] != expectedValue {
				t.Errorf("For key %q, expected value %q at index %d but got %q", key, expectedValue, i, values[i])
			}
		}
	}
}

func TestProcessFilesWritesIntermediateFilesToDisk(t *testing.T) {
	cfg := NewTestConfig()
	if err := os.RemoveAll(cfg.OutputDir); err != nil {
		t.Fatalf("failed to clear output directory %s: %v", cfg.OutputDir, err)
	}
	cfg.RegisterFn(&TestMapper{})

	intermediateData, err := processFiles(cfg)
	if err != nil {
		t.Fatalf("processFiles returned an error: %v", err)
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
			t.Fatalf("failed to read partition file %s: %v", partitionPath, err)
		}

		content := strings.TrimSpace(string(data))
		var got []string
		if content != "" {
			got = strings.Split(content, "\n")
		}

		expected := expectedByPartition[partition]
		if len(got) != len(expected) {
			t.Fatalf("partition %d: expected %d lines, got %d; contents=%q", partition, len(expected), len(got), string(data))
		}

		for i := range expected {
			if got[i] != expected[i] {
				t.Fatalf("partition %d: expected line %d to be %q, got %q", partition, i, expected[i], got[i])
			}
		}
	}
}

func TestProcessFilesCreatesOutputDirWhenMissing(t *testing.T) {
	cfg := NewTestConfig()
	cfg.OutputDir = filepath.Join(t.TempDir(), "nested", "mapper-output")
	cfg.RegisterFn(&TestMapper{})

	if _, err := processFiles(cfg); err != nil {
		t.Fatalf("processFiles returned an error: %v", err)
	}

	for partition := 0; partition < cfg.NumReducers; partition++ {
		partitionPath := filepath.Join(cfg.OutputDir, fmt.Sprintf("partition-%d", partition))
		if _, err := os.Stat(partitionPath); err != nil {
			t.Fatalf("expected partition file %s to exist: %v", partitionPath, err)
		}
	}
}

func TestProcessFilesRejectsInvalidReducerCount(t *testing.T) {
	cfg := NewTestConfig()
	cfg.OutputDir = t.TempDir()
	cfg.NumReducers = 0
	cfg.RegisterFn(&TestMapper{})

	_, err := processFiles(cfg)
	if err == nil {
		t.Fatal("expected processFiles to fail when NumReducers is zero")
	}
}
