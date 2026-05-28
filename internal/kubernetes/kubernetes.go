package kubernetes

import (
	"context"
	"path/filepath"
	"strconv"

	"github.com/mohammednumaan/shuffle/internal/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func CreateCluster(cfg *types.Config) *kubernetes.Clientset {
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

func GetNumOfNodes(clientset *kubernetes.Clientset) (int, error) {
	nodes, err := clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	return len(nodes.Items), nil
}

func CreateMapperJobSpec(jobId string, cfg *types.Config, inputSplit types.InputSplit, mapperId, outputPath string) *batchv1.Job {

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

func CreateReducerJobSpec(jobId string, cfg *types.Config, reducerId string, reducerIdx int, outputPath string) *batchv1.Job {
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
