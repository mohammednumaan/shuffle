package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/mapreduce"
	"github.com/mohammednumaan/shuffle/internal/types"
)

type SomeMap struct{}

func (sm *SomeMap) Map(key string, value string, emit types.Emitter) {
	for _, word := range strings.Fields(value) {
		if err := emit.Emit(word, "1"); err != nil {
			fmt.Printf("emit failed for input key %q: %v\n", key, err)
			return
		}
	}
}

func main() {
	// the user should provide the cli args as:
	// go run main.go -mode=master -input-dir=/mnt/nfs/test_dir -output-dir=/path/to/output -num-mappers=4 -num-reducers=2 -nfs-path=/path/to/nfs -image=mohammednumaan/mapreduce:latest
	cfg := config.SetupJobConfig()
	cfg.RegisterFn(&SomeMap{})
	jobId := fmt.Sprintf("job-%d", time.Now().Unix())
	mapreduce.ExecuteMapReduce(cfg, jobId)
}
