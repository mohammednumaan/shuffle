package mapper

import (
	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
	"strings"
	"testing"
)

func NewTestConfig() *config.Config {
	return &config.Config{
		InputDir:      "./test_files",
		FilePartition: "new_file2.txt,new_file4.txt",
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
	cfg.RegisterFn(&TestMapper{})
	intermediateData, err := processFiles(cfg)
	if err != nil {
		t.Fatalf("ProcessFiles returned an error: %v", err)
	}

	expected := map[string][]string{
		"kiwi":  {"1", "1"},
		"lime":  {"1", "1"},
		"mango": {"1", "1", "1"},
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
