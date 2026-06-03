package validation

import (
	"errors"
	"fmt"

	"github.com/mohammednumaan/shuffle/internal/types"
)

func checkNonEmpty(value, field string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	return nil
}

func checkPositive(value int, field string) error {
	if value <= 0 {
		return fmt.Errorf("%s must be greater than zero", field)
	}
	return nil
}

func checkTask(task *types.Task) error {
	if task == nil {
		return errors.New("task is nil")
	}
	return nil
}

func ValidateMasterRuntime(inputDirectory, outputDirectory string, numMachines int) error {
	if err := checkNonEmpty(inputDirectory, "input directory"); err != nil {
		return err
	}
	if err := checkNonEmpty(outputDirectory, "output directory"); err != nil {
		return err
	}
	if err := checkPositive(numMachines, "num machines"); err != nil {
		return err
	}
	return nil
}

func ValidateWorkerID(workerID string) error {
	return checkNonEmpty(workerID, "worker id")
}

func ValidateMapTask(task *types.Task) error {
	if err := checkTask(task); err != nil {
		return err
	}
	if task.Split == nil {
		return fmt.Errorf("map task %s split is nil", task.TaskId)
	}
	if err := checkPositive(task.NumReducers, "num reducers"); err != nil {
		return fmt.Errorf("map task %s has invalid num reducers %d", task.TaskId, task.NumReducers)
	}
	return nil
}

func ValidateReduceTask(task *types.Task) error {
	if err := checkTask(task); err != nil {
		return err
	}
	if task.ReducerIdx < 0 {
		return fmt.Errorf("reduce task %s has invalid reducer idx %d", task.TaskId, task.ReducerIdx)
	}
	if err := checkPositive(task.NumReducers, "num reducers"); err != nil {
		return fmt.Errorf("reduce task %s has invalid num reducers %d", task.TaskId, task.NumReducers)
	}
	if task.ReducerIdx >= task.NumReducers {
		return fmt.Errorf("reduce task %s reducer idx %d out of bounds for %d reducers", task.TaskId, task.ReducerIdx, task.NumReducers)
	}
	if err := checkNonEmpty(task.InputDir, "input dir"); err != nil {
		return fmt.Errorf("reduce task %s input dir is required", task.TaskId)
	}
	if err := checkNonEmpty(task.OutputDir, "output dir"); err != nil {
		return fmt.Errorf("reduce task %s output dir is required", task.TaskId)
	}
	return nil
}

func ValidateInputSplit(split *types.InputSplit) error {
	if split == nil {
		return errors.New("split is nil")
	}
	if err := checkNonEmpty(split.FilePath, "split file path"); err != nil {
		return err
	}
	if split.EndOffset < split.StartOffset {
		return errors.New("end offset must be greater than or equal to start offset")
	}
	return nil
}

func ValidateNumMappers(numMappers int) error {
	return checkPositive(numMappers, "num mappers")
}

func ValidateNumPartitions(numPartitions int) error {
	return checkPositive(numPartitions, "num partitions")
}

func ValidateOutputDir(outputDir string) error {
	return checkNonEmpty(outputDir, "output dir")
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
