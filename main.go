package main

import (
	"fmt"
	"time"

	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/mapreduce"
)

func main() {
	// the user should provide the cli args as:
	// go run main.go -mode=master -input-dir=/mnt/nfs/test_dir -output-dir=/path/to/output -num-mappers=4 -num-reducers=2 -nfs-path=/path/to/nfs -image=mohammednumaan/mapreduce:latest
	cfg := config.SetupJobConfig()
	jobId := fmt.Sprintf("job-%d", time.Now().Unix())

	mapreduce.ExecuteMapReduce(cfg, jobId)
}
