package validation

import (
	"errors"
	"fmt"

	"github.com/mohammednumaan/shuffle/internal/types"
)

func requireNonEmpty(value, field string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	return nil
}

func requirePositive(value int, field string) error {
	if value <= 0 {
		return fmt.Errorf("%s must be greater than zero", field)
	}
	return nil
}

func requireTask(task *types.Task) error {
	if task == nil {
		return errors.New("task is nil")
	}
	return nil
}

func ValidateMasterRuntime(inputDirectory, outputDirectory string, numMappers, numReducers int) error {
	if err := requireNonEmpty(inputDirectory, "input directory"); err != nil {
		return err
	}
	if err := requireNonEmpty(outputDirectory, "output directory"); err != nil {
		return err
	}
	if err := requirePositive(numMappers, "num mappers"); err != nil {
		return err
	}
	if err := requirePositive(numReducers, "num reducers"); err != nil {
		return err
	}
	return nil
}

func ValidateWorkerID(workerID string) error {
	return requireNonEmpty(workerID, "worker id")
}

func ValidateMapTask(task *types.Task) error {
	if err := requireTask(task); err != nil {
		return err
	}
	if task.Split == nil {
		return fmt.Errorf("map task %s split is nil", task.TaskId)
	}
	if err := requirePositive(task.NumReducers, "num reducers"); err != nil {
		return fmt.Errorf("map task %s has invalid num reducers %d", task.TaskId, task.NumReducers)
	}
	return nil
}

func ValidateReduceTask(task *types.Task) error {
	if err := requireTask(task); err != nil {
		return err
	}
	if task.ReducerIdx < 0 {
		return fmt.Errorf("reduce task %s has invalid reducer idx %d", task.TaskId, task.ReducerIdx)
	}
	if err := requirePositive(task.NumReducers, "num reducers"); err != nil {
		return fmt.Errorf("reduce task %s has invalid num reducers %d", task.TaskId, task.NumReducers)
	}
	if task.ReducerIdx >= task.NumReducers {
		return fmt.Errorf("reduce task %s reducer idx %d out of bounds for %d reducers", task.TaskId, task.ReducerIdx, task.NumReducers)
	}
	if err := requireNonEmpty(task.InputDir, "input dir"); err != nil {
		return fmt.Errorf("reduce task %s input dir is required", task.TaskId)
	}
	if err := requireNonEmpty(task.OutputDir, "output dir"); err != nil {
		return fmt.Errorf("reduce task %s output dir is required", task.TaskId)
	}
	return nil
}

func ValidateInputSplit(split *types.InputSplit) error {
	if split == nil {
		return errors.New("split is nil")
	}
	if err := requireNonEmpty(split.FilePath, "split file path"); err != nil {
		return err
	}
	if split.EndOffset < split.StartOffset {
		return errors.New("end offset must be greater than or equal to start offset")
	}
	return nil
}

func ValidateNumMappers(numMappers int) error {
	return requirePositive(numMappers, "num mappers")
}

func ValidateNumPartitions(numPartitions int) error {
	return requirePositive(numPartitions, "num partitions")
}

func ValidateOutputDir(outputDir string) error {
	return requireNonEmpty(outputDir, "output dir")
}

func ValidateMapper(mapper types.Mapper) error {
	if mapper == nil {
		return errors.New("mapper is nil")
	}
	return nil
}

func ValidateReducer(reducer types.Reducer) error {
	if reducer == nil {
		return errors.New("reducer is nil")
	}
	return nil
}
