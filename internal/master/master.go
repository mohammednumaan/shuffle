package master

// the master is the "orchestrator" of this entire framework
// it assigns "work"/"task" for the worker to complete
// these "workers" can be either a "mapper" or a "reducer"

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/mohammednumaan/shuffle/internal/config"
	"github.com/mohammednumaan/shuffle/internal/types"
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

func Run(cfg *config.Config, jobId string) {
	if err := validateMasterConfig(cfg); err != nil {
		log.Fatalf("invalid master config: %v", err)
	}

	clientset := createKubernetsCluster(cfg)
	numOfNodes := getNumOfNodes(clientset)

	if numOfNodes < cfg.NumMappers {
		log.Printf("warning: number of mappers (%d) is greater than number of nodes (%d)", cfg.NumMappers, numOfNodes)
	}

	master := NewMaster(jobId)
	partitions, err := BuildInputSplits(cfg.InputDir, cfg.SplitSizeMB*1024*1024)

	if err != nil {
		log.Fatalf("splitting input files failed: %v", err)
	}
	log.Printf("input splits: %+v", partitions)
	launchMapperWorkers(master, cfg, clientset, partitions)
	if err := waitForMappersToComplete(master, clientset); err != nil {
		log.Fatalf("mapper phase failed: %v", err)
	}
}

func validateMasterConfig(cfg *config.Config) error {
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

func getNumOfNodes(clientset *kubernetes.Clientset) int {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error fetching nodes: %v", err)
	}
	return len(nodes.Items)
}

func BuildInputSplits(inputDir string, splitSizeByte int64) ([]types.InputSplit, error) {
	if splitSizeByte <= 0 {
		return nil, errors.New("split size must be greater than zero")
	}

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read input directory: %w", err)
	}

	var splits []types.InputSplit
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(inputDir, entry.Name())
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat file %s: %w", filePath, err)
		}

		fileSize := fileInfo.Size()
		for start := int64(0); start < fileSize; start += splitSizeByte {
			end := start + splitSizeByte
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

func launchMapperWorkers(mt *Master, cfg *config.Config, clientset *kubernetes.Clientset, inputSplits []types.InputSplit) {
	for i := 0; i < len(inputSplits); i++ {
		jobId := mt.JobId
		mapperId := fmt.Sprintf("%s-mapper-%d", jobId, i)
		taskId := uuid.New().String()

		outputPath := filepath.Join(cfg.NfsPath, jobId, mapperId)
		task := types.Task{
			Id:             taskId,
			Type:           types.MapTask,
			Status:         types.Pending,
			Split:          inputSplits[i],
			AssignedWorker: mapperId,
			OutputPath:     outputPath,
		}
		mt.MapTasks = append(mt.MapTasks, task)

		job := createMapperJobSpec(jobId, cfg, inputSplits[i], mapperId, outputPath)
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})

		if err != nil {
			log.Printf("creating job for mapper %s failed: %v", mapperId, err)
			os.Exit(1)
		}
	}
}

func launchReducerWorkers(mt *Master, cfg *config.Config, clientset *kubernetes.Clientset) {
	for i := 0; i < cfg.NumReducers; i++ {
		jobId := mt.JobId
		reducerId := fmt.Sprintf("%s-reducer-%d", jobId, i)
		job := createReducerJobSpec(jobId, reducerId, cfg)

		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("creating job for reducer %s failed: %v", reducerId, err)
			os.Exit(1)
		}
	}
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
		task.Status = types.Running
	default:
		task.Status = types.Pending
	}
}

func waitForMappersToComplete(mt *Master, clientset *kubernetes.Clientset) error {
	for {
		allTasksCompleted := true
		for i := range mt.MapTasks {
			updateTaskProgress(&mt.MapTasks[i], clientset)
			switch mt.MapTasks[i].Status {
			case types.Completed:
				continue
			case types.Failed:
				return fmt.Errorf("mapper task %s assigned to %s failed", mt.MapTasks[i].Id, mt.MapTasks[i].AssignedWorker)
			default:
				allTasksCompleted = false
			}
		}

		log.Printf("mapper task statuses: %+v", mt.MapTasks)

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
func createReducerJobSpec(jobId string, reducerId string, cfg *config.Config) *batchv1.Job {
	inputPath := filepath.Join(cfg.NfsPath, jobId)
	outputPath := filepath.Join(cfg.NfsPath, jobId)

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
