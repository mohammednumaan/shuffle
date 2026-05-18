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

	"github.com/mohammednumaan/shuffle/internal/config"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FilePartition struct {
	StartFile string
	EndFile   string
	Files     []string
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

func Run(cfg *config.Config, jobId string) {
	// the masters job is to:
	// 1. create a clusters
	// 2. split the file and launch mappers
	clientset := createKubernetsCluster(cfg)
	numOfNodes := getNumOfNodes(clientset)

	if numOfNodes < cfg.NumMappers {
		log.Printf("Warning: Number of mappers (%d) is greater than number of nodes (%d).", cfg.NumMappers, numOfNodes)
	}

	partitions, err := SplitInputFiles(cfg.InputDir, cfg.NumMappers)
	if err != nil {
		log.Fatalf("Error splitting input files: %v", err)
	}
	log.Printf("File partitions: %+v", partitions)
	launchMapperWorkers(cfg, clientset, jobId, partitions)
}

func getNumOfNodes(clientset *kubernetes.Clientset) int {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error fetching nodes: %v", err)
	}
	return len(nodes.Items)
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

func launchMapperWorkers(cfg *config.Config, clientset *kubernetes.Clientset, jobId string, partitions []FilePartition) {
	for i := 0; i < cfg.NumMappers; i++ {
		mapperId := fmt.Sprintf("mapper-%d", i)
		log.Printf("Launching mapper worker: %s", mapperId)
		job := createMapperJobSpec(jobId, mapperId, cfg, partitions[i])
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Error creating job for mapper %s: %v", mapperId, err)
			os.Exit(1)
		}
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
								cfg.inputDir,
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
