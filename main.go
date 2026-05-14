package main

import (
	"log"
	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/master"
)

func main(){
	// the user should provide the cli args as:
	// go main -mode=master -input-dir=/path/to/input -output-dir=/path/to/output -num-mappers=4 -num-reducers=2 -nfs-path=/path/to/nfs -image=docker/image:tag
	cfg := config.SetupJobConfig()
	log.Printf("Configuration: %+v\n", cfg)

	master.Run(cfg)
}
