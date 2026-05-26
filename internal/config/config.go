package config

import (
	"flag"

	"github.com/mohammednumaan/shuffle/internal/types"
)

type Config struct {
	Mode        string
	InputDir    string
	OutputDir   string
	InputFile   string
	StartOffset int64
	EndOffset   int64
	NumMappers  int
	NumReducers int
	SplitSizeMB int64

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

	flag.StringVar(&cfg.Mode, "mode", "", "mode of operation: master, mapper, reducer")
	flag.StringVar(&cfg.InputDir, "input-dir", "", "path to the input directory")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "path to the output directory")
	flag.StringVar(&cfg.InputFile, "input-file", "", "path to the assigned input file for a mapper")
	flag.Int64Var(&cfg.StartOffset, "start-offset", 0, "start byte offset for the mapper input split")
	flag.Int64Var(&cfg.EndOffset, "end-offset", 0, "end byte offset for the mapper input split")
	flag.IntVar(&cfg.NumMappers, "num-mappers", 4, "number of mapper workers to use")
	flag.IntVar(&cfg.NumReducers, "num-reducers", 2, "number of reducer workers to use")
	flag.Int64Var(&cfg.SplitSizeMB, "split-size-mb", 32, "target mapper split size in mb")

	flag.StringVar(&cfg.NfsPath, "nfs-path", "/mnt/nfs", "path to the nfs mount")
	flag.StringVar(&cfg.Image, "image", "", "image that contains the worker binary")
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.Parse()

	return cfg
}
