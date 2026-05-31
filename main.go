package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/mohammednumaan/shuffle/internal/master"
	"github.com/mohammednumaan/shuffle/internal/worker"
)

func main() {
	mode := flag.String("mode", "", "mode of operation: master or worker")
	masterAddr := flag.String("master-addr", "127.0.0.1:9000", "master RPC address")
	jobID := flag.String("job-id", "", "job identifier (default generated)")
	inputDir := flag.String("input-dir", "", "path to the input directory")
	outputDir := flag.String("output-dir", "/tmp/shuffle/output", "path for reduce output")
	numMappers := flag.Int("num-mappers", 4, "number of mappers")
	numReducers := flag.Int("num-reducers", 2, "number of reducers")
	flag.Parse()

	if *jobID == "" {
		*jobID = fmt.Sprintf("job-%d", time.Now().Unix())
	}

	switch *mode {
	case "master":
		if *inputDir == "" {
			log.Fatal("input-dir is required in master mode")
		}
		if err := master.Run(*jobID, *masterAddr, *inputDir, *outputDir, *numMappers, *numReducers); err != nil {
			log.Fatalf("master: %v", err)
		}
		select {}
	case "worker":
		if err := worker.Run(*masterAddr); err != nil {
			log.Fatalf("worker: %v", err)
		}
	default:
		log.Fatalf("invalid mode %q, expected master or worker", *mode)
	}
}
