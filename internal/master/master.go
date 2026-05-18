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
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/mohammednumaan/shuffle/internal/config"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
)

type TaskStatus string

const (
	Pending   TaskStatus = "pending"
	Running   TaskStatus = "running"
	Completed TaskStatus = "completed"
	Failed    TaskStatus = "failed"
)

type FilePartition struct {
	StartFile string
	EndFile   string
	Files     []string
}

type Task struct {
	Id             string
	Type           TaskType
	Status         TaskStatus
	Partition      FilePartition
	AssignedWorker string
}

type Master struct {
	JobId       string
	MapTasks    []Task
	ReduceTasks []Task
}

func NewMaster(jobId string) *Master {
	return &Master{
		JobId:       jobId,
		MapTasks:    []Task{},
		ReduceTasks: []Task{},
	}
}

func NewTask(id string, taskType TaskType, partition FilePartition, workerId string) Task {
	return Task{
		Id:             id,
		Type:           taskType,
		Status:         Pending,
		Partition:      partition,
		AssignedWorker: workerId,
	}
}

func Run(cfg *config.Config, jobId string) {
	// the masters job is to:
	// 1. create a clusters
	// 2. split the file and launch mappers
	clientset := createKubernetsCluster(cfg)
	numOfNodes := getNumOfNodes(clientset)

	if numOfNodes < cfg.NumMappers {
		log.Printf("Warning: Number of mappers (%d) is greater than number of nodes (%d).", cfg.NumMappers, numOfNodes)
	}

	master := NewMaster(jobId)
	partitions, err := SplitInputFiles(cfg.InputDir, cfg.NumMappers)

	if err != nil {
		log.Fatalf("Error splitting input files: %v", err)
	}
	log.Printf("File partitions: %+v", partitions)
	launchMapperWorkers(master, cfg, clientset, jobId, partitions)
	if err := waitForMappersToComplete(master, clientset); err != nil {
		log.Fatalf("Mapper phase failed: %v", err)
	}
}

func getNumOfNodes(clientset *kubernetes.Clientset) int {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error fetching nodes: %v", err)
	}
	return len(nodes.Items)
}

func SplitInputFiles(inputPath string, numOfMapTasks int) ([]FilePartition, error) {
	if numOfMapTasks <= 0 {
		return nil, errors.New("number of map tasks must be greater than 0")
	}

	entries, err := os.ReadDir(inputPath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to read input directory: %v", err))
	}

	files := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// i know the input dir path so using entry.Name() is fine here
		files = append(files, entry.Name())
	}

	if len(files) == 0 {
		return []FilePartition{}, nil
	}

	if numOfMapTasks > len(files) {
		numOfMapTasks = len(files)
	}

	sort.Strings(files)

	// now i split the files into ranges and the total ranges are M (= numOfMapTasks)
	totalFilesForEachWorker := len(files) / numOfMapTasks
	extraFiles := len(files) % numOfMapTasks
	filePartitions := make([]FilePartition, 0, numOfMapTasks)
	fmt.Printf("Total files: %d, Files per worker: %d, Extra files: %d\n", len(files), totalFilesForEachWorker, extraFiles)

	start := 0
	for i := 0; i < numOfMapTasks; i++ {
		size := totalFilesForEachWorker
		if extraFiles > 0 {
			size++
			extraFiles--
		}

		end := start + size
		if end > len(files) {
			end = len(files)
		}

		filesInCurrentRange := files[start:end]
		filePartitions = append(filePartitions, FilePartition{
			StartFile: filesInCurrentRange[0],
			EndFile:   filesInCurrentRange[len(filesInCurrentRange)-1],
			Files:     filesInCurrentRange,
		})

		start = end
	}

	return filePartitions, nil

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

func launchMapperWorkers(mt *Master, cfg *config.Config, clientset *kubernetes.Clientset, jobId string, partitions []FilePartition) {
	for i := 0; i < len(partitions); i++ {
		mapperId := fmt.Sprintf("mapper-%d", i)
		taskId := uuid.New().String()
		task := NewTask(taskId, MapTask, partitions[i], mapperId)

		mt.MapTasks = append(mt.MapTasks, task)
		job := createMapperJobSpec(jobId, mapperId, cfg, partitions[i])
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Error creating job for mapper %s: %v", mapperId, err)
			os.Exit(1)
		}
	}
}

func updateTaskProgress(task *Task, clientset *kubernetes.Clientset) {
	assignedWorker := task.AssignedWorker
	job, err := clientset.BatchV1().Jobs("default").Get(context.TODO(), assignedWorker, metav1.GetOptions{})
	if err != nil {
		log.Printf("Error fetching job status for worker %s: %v", assignedWorker, err)
		task.Status = Failed
		return
	}

	switch {
	case job.Status.Succeeded > 0:
		task.Status = Completed
	case job.Status.Failed > 0:
		task.Status = Failed
	case job.Status.Active > 0:
		task.Status = Running
	default:
		task.Status = Pending
	}
}

func waitForMappersToComplete(mt *Master, clientset *kubernetes.Clientset) error {
	for {
		allTasksCompleted := true
		for i := range mt.MapTasks {
			updateTaskProgress(&mt.MapTasks[i], clientset)
			switch mt.MapTasks[i].Status {
			case Completed:
				continue
			case Failed:
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

func createMapperJobSpec(jobId string, mapperId string, cfg *config.Config, partition FilePartition) *batchv1.Job {

	outputPath := filepath.Join(cfg.NfsPath, jobId, mapperId)
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
								"--input-dir",
								cfg.InputDir,
								"--output-dir",
								outputPath,
								"--file-partition",
								fmt.Sprintf("%s-%s", partition.StartFile, partition.EndFile),
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

func launchReducerWorkers(cfg *config.Config, clientset *kubernetes.Clientset, jobId string) {
	for i := 0; i < cfg.NumReducers; i++ {
		reducerId := fmt.Sprintf("reducer-%d", i)
		log.Printf("Launching reducer worker: %s", reducerId)
		job := createReducerJobSpec(jobId, reducerId, cfg)
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Error creating job for reducer %s: %v", reducerId, err)
			os.Exit(1)
		}
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
