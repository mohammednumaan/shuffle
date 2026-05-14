package master

// the master is the "orchestrator" of this entire framework
// it assigns "work"/"task" for the worker to complete
// these "workers" can be either a "mapper" or a "reducer"

import (
	"context"
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

func Run(cfg *config.Config, jobId string) {

	log.Printf("in master.Run with config: %+v\n", cfg)
	clientset := createKubernetsCluster(cfg)
	numOfNodes := getNumOfNodes(clientset)
	log.Printf("Master is running...")
	log.Printf("Number of nodes in the cluster: %d", numOfNodes)

	launchMapperWorkers(cfg, clientset, jobId)
	log.Printf("Launched %d mapper workers", cfg.NumMappers)
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

func launchMapperWorkers(cfg *config.Config, clientset *kubernetes.Clientset, jobId string) {
	for i := 0; i < cfg.NumMappers; i++ {
		mapperId := fmt.Sprintf("mapper-%d", i)
		log.Printf("Launching mapper worker: %s", mapperId)
		job := createMapperJobSpec(jobId, mapperId, cfg)
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Error creating job for mapper %s: %v", mapperId, err)
			os.Exit(1)
		}
	}
}

func createMapperJobSpec(jobId string, mapperId string, cfg *config.Config) *batchv1.Job {
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
