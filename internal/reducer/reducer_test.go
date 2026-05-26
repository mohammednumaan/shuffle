package reducer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
)

type testReducer struct{}

func (tr *testReducer) Reduce(key string, values []string) (string, error) {
	return fmt.Sprintf("%d", len(values)), nil
}

func createPartitionRecords(t *testing.T, path string, records []types.IntermediateRecord) {
	t.Helper()

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating partition file failed: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatalf("encoding partition record failed: %v", err)
		}
	}
}

func TestReducer(t *testing.T) {
	inputDir := t.TempDir()
	worker0 := filepath.Join(inputDir, "worker-0")
	if err := os.MkdirAll(worker0, 0o755); err != nil {
		t.Fatalf("creating worker-0 dir failed: %v", err)
	}

	worker1 := filepath.Join(inputDir, "worker-1")
	if err := os.MkdirAll(worker1, 0o755); err != nil {
		t.Fatalf("creating worker-1 dir failed: %v", err)
	}

	createPartitionRecords(t, filepath.Join(worker0, "partition-1"), []types.IntermediateRecord{
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "1"},
	})

	createPartitionRecords(t, filepath.Join(worker1, "partition-1"), []types.IntermediateRecord{
		{Key: "apple", Value: "1"},
		{Key: "carrot", Value: "1"},
	})

	createPartitionRecords(t, filepath.Join(worker1, "partition-0"), []types.IntermediateRecord{
		{Key: "mango", Value: "1"},
	})

	outputDir := filepath.Join(t.TempDir(), "output")
	cfg := &config.Config{
		InputDir:    inputDir,
		OutputDir:   outputDir,
		NumReducers: 3,
		ReducerIdx:  1,
		Reducer:     &testReducer{},
	}

	Run(cfg)
	outputPath := filepath.Join(outputDir, "reducer-1")

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("reading reducer output failed: %v", err)
	}

	got := strings.TrimSpace(string(data))
	expected := "apple,2\nbanana,1\ncarrot,1"

	if got != expected {
		t.Fatalf("expected reducer output:\n%s\n\ngot:\n%s", expected, got)
	}
}

func TestRunOnlyProcessesAssignedPartition(t *testing.T) {
	inputDir := t.TempDir()

	worker0 := filepath.Join(inputDir, "worker-0")
	if err := os.MkdirAll(worker0, 0o755); err != nil {
		t.Fatalf("creating worker-0 dir failed: %v", err)
	}

	worker1 := filepath.Join(inputDir, "worker-1")
	if err := os.MkdirAll(worker1, 0o755); err != nil {
		t.Fatalf("creating worker-1 dir failed: %v", err)
	}

	createPartitionRecords(t, filepath.Join(worker0, "partition-1"), []types.IntermediateRecord{
		{Key: "apple", Value: "1"},
	})

	createPartitionRecords(t, filepath.Join(worker1, "partition-0"), []types.IntermediateRecord{
		{Key: "banana", Value: "1"},
	})

	outputDir := filepath.Join(t.TempDir(), "output")
	cfg := &config.Config{
		InputDir:    inputDir,
		OutputDir:   outputDir,
		NumReducers: 3,
		ReducerIdx:  1,
		Reducer:     &testReducer{},
	}

	Run(cfg)
	outputPath := filepath.Join(outputDir, "reducer-1")

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("reading reducer output failed: %v", err)
	}

	got := strings.TrimSpace(string(data))
	expected := "apple,1"
	if got != expected {
		t.Fatalf("expected reducer output:\n%s\n\ngot:\n%s", expected, got)
	}
}

func TestReducerReadsJSONEncodedSpecialCharacters(t *testing.T) {
	inputDir := t.TempDir()
	worker0 := filepath.Join(inputDir, "worker-0")
	if err := os.MkdirAll(worker0, 0o755); err != nil {
		t.Fatalf("creating worker-0 dir failed: %v", err)
	}

	createPartitionRecords(t, filepath.Join(worker0, "partition-0"), []types.IntermediateRecord{
		{Key: "alpha,beta", Value: "first\nline"},
		{Key: "alpha,beta", Value: "second"},
	})

	outputDir := filepath.Join(t.TempDir(), "output")
	cfg := &config.Config{
		InputDir:    inputDir,
		OutputDir:   outputDir,
		NumReducers: 1,
		ReducerIdx:  0,
		Reducer:     &testReducer{},
	}

	Run(cfg)

	data, err := os.ReadFile(filepath.Join(outputDir, "reducer-0"))
	if err != nil {
		t.Fatalf("reading reducer output failed: %v", err)
	}

	got := strings.TrimSpace(string(data))
	expected := "alpha,beta,2"
	if got != expected {
		t.Fatalf("expected reducer output:\n%s\n\ngot:\n%s", expected, got)
	}
}
