package config

import "flag"

type Config struct {
	Mode string
	InputDir string
	OutputDir string
	NumMappers int
	NumReducers int

	NfsPath string
	Image string
}

func SetupJobConfig() *Config {

	cfg := &Config{}

	flag.StringVar(&cfg.Mode, "mode", "", "Mode of operation: master, mapper, reducer.")
	flag.StringVar(&cfg.InputDir, "input-dir", "", "Path to input directory.")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "Path to output directory.")
	flag.IntVar(&cfg.NumMappers, "num-mappers", 0, "Number of mappers to use.")
	flag.IntVar(&cfg.NumReducers, "num-reducers", 0, "Number of reducers to use.")

	flag.StringVar(&cfg.NfsPath, "nfs-path", "", "Path to NFS directory.")
	flag.StringVar(&cfg.Image, "image", "", "Docker image to use for mappers and reducers.")
	flag.Parse()

	return cfg

}
