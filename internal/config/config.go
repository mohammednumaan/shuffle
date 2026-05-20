package config

import (
	"flag"
	"github.com/mohammednumaan/shuffle/internal/types"
)

type Config struct {
	Mode          string
	InputDir      string
	OutputDir     string
	FilePartition string
	NumMappers    int
	NumReducers   int

	NfsPath    string
	Image      string
	Kubeconfig string

	Mapper types.Mapper
}

func (cfg *Config) RegisterFn(mapperFn types.Mapper) {
	cfg.Mapper = mapperFn
}

func SetupJobConfig() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.Mode, "mode", "", "Mode of operation: master, mapper, reducer.")
	flag.StringVar(&cfg.InputDir, "input-dir", "", "Path to input directory.")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "Path to output directory.")
	flag.IntVar(&cfg.NumMappers, "num-mappers", 4, "Number of mappers to use.")
	flag.IntVar(&cfg.NumReducers, "num-reducers", 2, "Number of reducers to use.")
	flag.StringVar(&cfg.FilePartition, "file-partition", "", "File partition for mapper workers (e.g., '0-99').")

	flag.StringVar(&cfg.NfsPath, "nfs-path", "/mnt/nfs", "Path to NFS directory.")
	flag.StringVar(&cfg.Image, "image", "", "image to use for mapper and reducer workers.")
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Absolute path to kubeconfig file.")
	flag.Parse()

	return cfg
}
