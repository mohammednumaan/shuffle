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
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/utils"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func Run(cfg *config.Config, jobId string) error {
	if err := utils.ValidateMasterConfig(cfg); err != nil {
		return fmt.Errorf("master config: %w", err)
	}

	clientset := createKubernetsCluster(cfg)
	numOfNodes, err := getNumOfNodes(clientset)
	if err != nil {
		return fmt.Errorf("getting nodes: %w", err)
	}

	if numOfNodes < cfg.NumMappers {
		log.Printf("warning: number of mappers (%d) is greater than number of nodes (%d)", cfg.NumMappers, numOfNodes)
	}

	master := NewMaster(jobId)
	partitions, err := BuildInputSplitsForMappers(cfg.InputDir, cfg.NumMappers)
	if err != nil {
		return fmt.Errorf("building input splits: %w", err)
	}
	log.Printf("input splits: %+v", partitions)

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

func getNumOfNodes(clientset *kubernetes.Clientset) (int, error) {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("fetching nodes: %w", err)
	}
	return len(nodes.Items), nil
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

func createKubernetsCluster(cfg *config.Config) *kubernetes.Clientset {
	kubeconfig := cfg.Kubeconfig
	if kubeconfig == "" {
		if home := homedir.HomeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)

	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return clientset
}

func launchMapperWorkers(mt *Master, cfg *config.Config, clientset *kubernetes.Clientset, inputSplits []types.InputSplit) error {
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

		job := createMapperJobSpec(jobId, cfg, inputSplits[i], mapperId, outputPath)
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})

		if err != nil {
			return fmt.Errorf("creating job for mapper %s: %w", mapperId, err)
		}
	}

	return nil
}

func launchReducerWorkers(mt *Master, cfg *config.Config, clientset *kubernetes.Clientset) error {
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
		job := createReducerJobSpec(jobId, cfg, reducerId, i, outputPath)

		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating job for reducer %s: %w", reducerId, err)
		}
	}

	return nil
}

func updateTaskProgress(task *types.Task, clientset *kubernetes.Clientset) {

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

func handleTaskFailure(task *types.Task, mt *Master, clientset *kubernetes.Clientset) error {

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
	task.RetryCount++
	task.Status = types.Idle
	task.AssignedWorker = ""
	task.OutputPath = ""

	return nil
}

func rescheduleIdleTasks(cfg *config.Config, mt *Master, clientset *kubernetes.Clientset) error {
	for _, task := range mt.MapTasks {
		if task.Status == types.Idle {

			newMapperId := fmt.Sprintf("%s-mapper-%s-retry-%d", mt.JobId, uuid.New().String(), task.RetryCount)
			jobId := mt.JobId

			task.OutputPath = filepath.Join(cfg.NfsPath, jobId, newMapperId)
			task.AssignedWorker = newMapperId
			job := createMapperJobSpec(jobId, cfg, task.Split, newMapperId, task.OutputPath)

			_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("creating job for mapper %s: %w", newMapperId, err)
			}

			task.Status = types.Idle
		}
	}

	return nil

}

func waitForMappersToComplete(cfg *config.Config, mt *Master, clientset *kubernetes.Clientset) error {
	// i constantly poll the status of the mapper to
	// know when they are done, if they failed, or if they are still in progress
	for {
		allTasksCompleted := true
		for i := range mt.MapTasks {
			updateTaskProgress(&mt.MapTasks[i], clientset)

			switch mt.MapTasks[i].Status {
			case types.Completed:
				continue

			case types.Failed:

				// to make this fault-tolerant, if a mapper task failed, i want to retry it on another worker
				// so first i keep retrying the task in different jobs machines
				// if the task exceeds the maax retries, i return an error
				if err := handleTaskFailure(&mt.MapTasks[i], mt, clientset); err != nil {
					return fmt.Errorf("handling failure for task %s: %w", mt.MapTasks[i].Id, err)
				}

				allTasksCompleted = false

			default:
				allTasksCompleted = false
			}
		}

		log.Printf("mapper task statuses: %+v", mt.MapTasks)

		if allTasksCompleted {
			return nil
		}

		if err := rescheduleIdleTasks(cfg, mt, clientset); err != nil {
			return fmt.Errorf("rescheduling idle tasks: %w", err)
		}

		time.Sleep(2 * time.Second)
	}
}

func waitForReducersToComplete(mt *Master, clientset *kubernetes.Clientset) error {
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

func createMapperJobSpec(jobId string, cfg *config.Config, inputSplit types.InputSplit, mapperId, outputPath string) *batchv1.Job {

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mapperId,
			Namespace: "default",
			Labels: map[string]string{
				"job-group": jobId + "-mapper",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"job-group": jobId + "-mapper",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: cfg.Image,
							Command: []string{
								"./mapreduce",
								"--mode",
								"mapper",
								"--num-reducers",
								strconv.Itoa(cfg.NumReducers),
								"--input-file",
								inputSplit.FilePath,
								"--output-dir",
								outputPath,
								"--start-offset",
								strconv.FormatInt(inputSplit.StartOffset, 10),
								"--end-offset",
								strconv.FormatInt(inputSplit.EndOffset, 10),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "nfs-volume",
									MountPath: cfg.NfsPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "nfs-volume",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "nfs-pvc",
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
}
func createReducerJobSpec(jobId string, cfg *config.Config, reducerId string, reducerIdx int, outputPath string) *batchv1.Job {
	inputPath := filepath.Join(cfg.NfsPath, jobId)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reducerId,
			Namespace: "default",
			Labels: map[string]string{
				"job-group": jobId + "-reducer",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"job-group": jobId + "-reducer",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: cfg.Image,
							Command: []string{
								"./mapreduce",
								"--mode",
								"reducer",
								"--num-reducers",
								strconv.Itoa(cfg.NumReducers),
								"--reducer-idx",
								strconv.Itoa(reducerIdx),
								"--input-dir",
								inputPath,
								"--output-dir",
								outputPath,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "nfs-volume",
									MountPath: cfg.NfsPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "nfs-volume",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "nfs-pvc",
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
}
