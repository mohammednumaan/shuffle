package master

// the master is the "orchestrator" of this entire framework
// it assigns "work"/"task" for the worker to complete
// these "workers" can be either a "mapper" or a "reducer"

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/mohammednumaan/shuffle/internal/kubernetes"
	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/validation"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
)

type Master struct {
	JobId       string
	MapTasks    []types.Task
	ReduceTasks []types.Task
}

func NewMaster(jobId string) *Master {
	return &Master{
		JobId:       jobId,
		MapTasks:    []types.Task{},
		ReduceTasks: []types.Task{},
	}
}

func Run(cfg *types.Config, jobId string) error {
	if err := validation.ValidateMasterConfig(cfg); err != nil {
		return fmt.Errorf("master config: %w", err)
	}

	clientset := kubernetes.CreateCluster(cfg)
	master := NewMaster(jobId)

	partitions, err := BuildInputSplitsForMappers(cfg.InputDir, cfg.NumMappers)
	if err != nil {
		return fmt.Errorf("building input splits: %w", err)
	}

	if err := launchMapperWorkers(master, cfg, clientset, partitions); err != nil {
		return fmt.Errorf("launching mapper workers: %w", err)
	}

	if err := waitForMappersToComplete(cfg, master, clientset); err != nil {
		return fmt.Errorf("mapper phase: %w", err)
	}

	if err := launchReducerWorkers(master, cfg, clientset); err != nil {
		return fmt.Errorf("launching reducer workers: %w", err)
	}

	if err := waitForReducersToComplete(master, clientset); err != nil {
		return fmt.Errorf("reducer phase: %w", err)
	}

	return nil
}

func BuildInputSplitsForMappers(inputDir string, numMappers int) ([]types.InputSplit, error) {
	if numMappers <= 0 {
		return nil, errors.New("num mappers must be greater than zero")
	}

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	var filePaths []string
	var totalSize int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(inputDir, entry.Name())
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("stat file %s: %w", filePath, err)
		}

		if fileInfo.Size() == 0 {
			continue
		}

		filePaths = append(filePaths, filePath)
		totalSize += fileInfo.Size()
	}

	if totalSize == 0 {
		return []types.InputSplit{}, nil
	}

	targetSplitSize := int64(math.Ceil(float64(totalSize) / float64(numMappers)))
	if targetSplitSize <= 0 {
		targetSplitSize = 1
	}

	var splits []types.InputSplit
	for _, filePath := range filePaths {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("stat file %s: %w", filePath, err)
		}

		fileSize := fileInfo.Size()
		for start := int64(0); start < fileSize; start += targetSplitSize {
			end := start + targetSplitSize
			if end > fileSize {
				end = fileSize
			}

			splits = append(splits, types.InputSplit{
				FilePath:    filePath,
				StartOffset: start,
				EndOffset:   end,
			})
		}
	}

	return splits, nil
}

func launchMapperWorkers(mt *Master, cfg *types.Config, clientset *k8sclient.Clientset, inputSplits []types.InputSplit) error {
	for i := 0; i < len(inputSplits); i++ {
		jobId := mt.JobId
		mapperId := fmt.Sprintf("%s-mapper-%d", jobId, i)
		taskId := uuid.New().String()

		outputPath := filepath.Join(cfg.NfsPath, jobId, mapperId)
		task := types.Task{
			Id:         taskId,
			Type:       types.MapTask,
			Status:     types.Idle,
			RetryCount: 0,
			MaxRetries: 3,
		}
		mt.MapTasks = append(mt.MapTasks, task)

		job := kubernetes.CreateMapperJobSpec(jobId, cfg, inputSplits[i], mapperId, outputPath)
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})

		if err != nil {
			return fmt.Errorf("creating job for mapper %s: %w", mapperId, err)
		}
	}

	return nil
}

func launchReducerWorkers(mt *Master, cfg *types.Config, clientset *k8sclient.Clientset) error {
	for i := 0; i < cfg.NumReducers; i++ {
		jobId := mt.JobId
		reducerId := fmt.Sprintf("%s-reducer-%d", jobId, i)

		taskId := uuid.New().String()
		outputPath := filepath.Join(cfg.NfsPath, jobId, "output")

		task := types.Task{
			Id:         taskId,
			Type:       types.ReduceTask,
			Status:     types.Idle,
			RetryCount: 0,
			MaxRetries: 3,
		}

		mt.ReduceTasks = append(mt.ReduceTasks, task)
		job := kubernetes.CreateReducerJobSpec(jobId, cfg, reducerId, i, outputPath)

		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating job for reducer %s: %w", reducerId, err)
		}
	}

	return nil
}

func updateTaskProgress(task *types.Task, clientset *k8sclient.Clientset) {

	assignedWorker := task.AssignedWorker
	job, err := clientset.BatchV1().Jobs("default").Get(context.TODO(), assignedWorker, metav1.GetOptions{})

	if err != nil {
		log.Printf("fetching job status for worker %s failed: %v", assignedWorker, err)
		task.Status = types.Failed
		return
	}

	switch {
	case job.Status.Succeeded > 0:
		task.Status = types.Completed
	case job.Status.Failed > 0:
		task.Status = types.Failed
	case job.Status.Active > 0:
		task.Status = types.InProgress
	default:
		task.Status = types.Idle
	}
}

func handleTaskFailure(task *types.Task, mt *Master, clientset *k8sclient.Clientset) error {

	// step 1: check if the task has exceeded allowd retries
	// if it did, i return an error
	if task.RetryCount >= task.MaxRetries {
		return fmt.Errorf("task %s failed after %d retries", task.Id, task.RetryCount)
	}

	log.Printf("task %s failed, retrying (%d/%d)", task.Id, task.RetryCount+1, task.MaxRetries)

	// step 2: delete/clean up the failed job
	deletePolicy := metav1.DeletePropagationBackground
	err := clientset.BatchV1().Jobs("default").Delete(context.TODO(), task.AssignedWorker, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})

	if err != nil {
		log.Printf("failed to delete job for worker %s: %v", task.AssignedWorker, err)
	}

	// step 3: i also need to cleanup the output directory
	// that the failed worker might have written to
	if task.Type == types.MapTask {
		outputDir := task.OutputPath
		err := os.RemoveAll(outputDir)
		if err != nil {
			return fmt.Errorf("cleaning up output directory %s: %w", outputDir, err)
		}
	}

	// step 4: reset the task status to idle so that
	// the master can pick it up and assign it to a new worker
	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second

	task.Status = types.Idle
	task.AssignedWorker = ""
	task.OutputPath = ""

	task.RetryCount++
	task.RetryAfter = time.Now().Add(exponentialBackoffWithJitter(task.RetryCount, baseDelay, maxDelay))

	return nil
}

func rescheduleIdleTasks(cfg *types.Config, mt *Master, clientset *k8sclient.Clientset) error {
	now := time.Now()
	for _, task := range mt.MapTasks {
		if task.Status == types.Idle {

			if !now.After(task.RetryAfter) && task.RetryCount > 0 {
				continue
			}

			newMapperId := fmt.Sprintf("%s-mapper-%s-retry-%d", mt.JobId, uuid.New().String(), task.RetryCount)
			jobId := mt.JobId

			task.OutputPath = filepath.Join(cfg.NfsPath, jobId, newMapperId)
			task.AssignedWorker = newMapperId
			job := kubernetes.CreateMapperJobSpec(jobId, cfg, task.Split, newMapperId, task.OutputPath)

			_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("creating job for mapper %s: %w", newMapperId, err)
			}

			task.Status = types.InProgress
		}
	}

	return nil

}

func waitForMappersToComplete(cfg *types.Config, mt *Master, clientset *k8sclient.Clientset) error {
	for {
		allTasksCompleted := true
		for i := range mt.MapTasks {
			updateTaskProgress(&mt.MapTasks[i], clientset)

			switch mt.MapTasks[i].Status {
			case types.Completed:
				continue

			case types.Failed:
				if err := handleTaskFailure(&mt.MapTasks[i], mt, clientset); err != nil {
					return fmt.Errorf("handling failure for task %s: %w", mt.MapTasks[i].Id, err)
				}

				allTasksCompleted = false

			default:
				allTasksCompleted = false
			}
		}

		if allTasksCompleted {
			return nil
		}

		if err := rescheduleIdleTasks(cfg, mt, clientset); err != nil {
			return fmt.Errorf("rescheduling idle tasks: %w", err)
		}

		time.Sleep(2 * time.Second)
	}
}

func waitForReducersToComplete(mt *Master, clientset *k8sclient.Clientset) error {
	for {
		allTasksCompleted := true
		for i := range mt.ReduceTasks {
			updateTaskProgress(&mt.ReduceTasks[i], clientset)
			switch mt.ReduceTasks[i].Status {
			case types.Completed:
				continue
			case types.Failed:
				return fmt.Errorf("reducer task %s assigned to %s failed", mt.ReduceTasks[i].Id, mt.ReduceTasks[i].AssignedWorker)
			default:
				allTasksCompleted = false
			}
		}

		log.Printf("reducer task statuses: %+v", mt.ReduceTasks)

		if allTasksCompleted {
			return nil
		}

		time.Sleep(2 * time.Second)
	}
}

func exponentialBackoffWithJitter(retryAttempt int, baseDelay, maxDelay time.Duration) time.Duration {

	delay := baseDelay * (1 << retryAttempt)
	if delay > maxDelay {
		delay = maxDelay
	}

	jitter := time.Duration(rand.Int63n(int64(baseDelay / 2)))
	return delay + jitter
}
