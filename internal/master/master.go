package master

// the master is the "orchestrator" of this entire framework
// it assigns "work"/"task" for the worker to complete
// these "workers" can be either a "mapper" or a "reducer"

import (

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"flag"
	"log"
	"path/filepath"
	"context"

	"github.com/mohammednumaan/shuffle/internal/config"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)
func Run(cfg *config.Config) {
	clientset := createKubernetsCluster()
	numOfNodes :=  getNumOfNodes(clientset)
	log.Printf("Master is running in %s mode", cfg.Mode)
	log.Printf("Number of nodes in the cluster: %d", numOfNodes)
}

func getNumOfNodes(clientset *kubernetes.Clientset) int {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error fetching nodes: %v", err)
	}
	return len(nodes.Items)
}

func createKubernetsCluster() *kubernetes.Clientset {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) abs path to kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "abs path to kubeconfig file")
	}

	flag.Parse()
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)

	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return clientset
}
