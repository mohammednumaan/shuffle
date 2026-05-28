package utils

import (
	"errors"

	"github.com/mohammednumaan/shuffle/internal/config"
)

// ValidateMasterConfig validates the configuration for the master process
func ValidateMasterConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if cfg.InputDir == "" {
		return errors.New("input dir is required")
	}
	if cfg.NfsPath == "" {
		return errors.New("nfs path is required")
	}
	if cfg.Image == "" {
		return errors.New("image is required")
	}
	if cfg.NumReducers <= 0 {
		return errors.New("num reducers must be greater than zero")
	}
	if cfg.NumMappers <= 0 {
		return errors.New("num mappers must be greater than zero")
	}
	if cfg.SplitSizeMB <= 0 {
		return errors.New("split size mb must be greater than zero")
	}

	return nil
}

// ValidateMapperConfig validates the configuration for mapper processes
func ValidateMapperConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}

	if cfg.InputFile == "" {
		return errors.New("input file is required")
	}

	if cfg.OutputDir == "" {
		return errors.New("output dir is required")
	}

	if cfg.EndOffset < cfg.StartOffset {
		return errors.New("end offset must be greater than or equal to start offset")
	}

	if cfg.Mapper == nil {
		return errors.New("mapper is nil")
	}

	return nil
}

// ValidateReducerConfig validates the configuration for reducer processes
func ValidateReducerConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}

	if cfg.InputDir == "" {
		return errors.New("input dir is required")
	}

	if cfg.OutputDir == "" {
		return errors.New("output dir is required")
	}

	if cfg.NumReducers <= 0 {
		return errors.New("number of reducers must be greater than 0")
	}

	if cfg.ReducerIdx < 0 || cfg.ReducerIdx >= cfg.NumReducers {
		return errors.New("invalid reducer index")
	}

	if cfg.Reducer == nil {
		return errors.New("reducer is nil")
	}
	return nil
}
