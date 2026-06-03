package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/mohammednumaan/shuffle/internal/master"
	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/worker"
)

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

func main() {
	mode := flag.String("mode", "", "mode of operation: master or worker")
	masterAddr := flag.String("master-addr", "127.0.0.1:9000", "master RPC address")
	jobID := flag.String("job-id", "", "job identifier (default generated)")
	inputDir := flag.String("input-dir", "", "path to the input directory")
	outputDir := flag.String("output-dir", "/tmp/shuffle/output", "path for reduce output")
	numMachines := flag.Int("num-machines", 4, "number of machines in the cluster")
	flag.Parse()

	if *jobID == "" {
		*jobID = fmt.Sprintf("job-%d", time.Now().Unix())
	}

	switch *mode {
	case "master":
		if *inputDir == "" {
			log.Fatal("input-dir is required in master mode")
		}
		if err := master.Run(*jobID, *masterAddr, *inputDir, *outputDir, *numMachines); err != nil {
			log.Fatalf("master: %v", err)
		}
	case "worker":
		if err := worker.RegisterFunctions(defaultMapper{}, defaultReducer{}); err != nil {
			log.Fatalf("register functions: %v", err)
		}
		if err := worker.Run(*masterAddr); err != nil {
			log.Fatalf("worker: %v", err)
		}
	default:
		log.Fatalf("invalid mode %q, expected master or worker", *mode)
	}
}
