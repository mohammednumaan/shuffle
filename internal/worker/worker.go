package worker

import (
	"fmt"
	"log"
	"net"
	netrpc "net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
	shufflerpc "github.com/mohammednumaan/shuffle/internal/rpc"
	"github.com/mohammednumaan/shuffle/internal/types"
)

type WorkerRPC struct{}

func (w *WorkerRPC) FetchPartition(args *shufflerpc.FetchPartitionArgs, reply *shufflerpc.FetchPartitionReply) error {
	data, err := os.ReadFile(args.FilePath)
	if err != nil {
		reply.Error = fmt.Sprintf("read partition file: %v", err)
		return nil
	}
	reply.Data = data
	return nil
}

// this will be called by the master to constantly poll
// if this specific worker is active (for fault tolerance)
func (w *WorkerRPC) Ping(args *shufflerpc.PingArgs, reply *shufflerpc.PingReply) error {
	return nil
}

func StartWorkerRPCServer(addr string) error {
	if err := netrpc.Register(&WorkerRPC{}); err != nil {
		return fmt.Errorf("register worker rpc: %w", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Printf("worker rpc server listening on %s", addr)
	go netrpc.Accept(ln)
	return nil
}

const workerRPCPort = ":5001"

func Run(masterAddress string) error {
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		podIP = "127.0.0.1"
		log.Printf("POD_IP not set, defaulting to %s", podIP)
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = uuid.New().String()
		log.Printf("WORKER_ID not set, generated: %s", workerID)
	}

	workerAddress := podIP + workerRPCPort

	if err := StartWorkerRPCServer("0.0.0.0" + workerRPCPort); err != nil {
		return fmt.Errorf("start worker rpc server: %w", err)
	}

	client, err := netrpc.Dial("tcp", masterAddress)
	if err != nil {
		return fmt.Errorf("dial master: %w", err)
	}
	defer client.Close()

	registerArgs := &shufflerpc.RegisterWorkerArgs{
		WorkerId: workerID,
		Address:  workerAddress,
	}
	var registerReply shufflerpc.RegisterWorkerReply
	if err := client.Call("Master.RegisterWorker", registerArgs, &registerReply); err != nil {
		return fmt.Errorf("register worker rpc: %w", err)
	}
	if registerReply.Error != "" {
		return fmt.Errorf("register worker error: %s", registerReply.Error)
	}
	log.Printf("[Worker %s] registered with master at %s", workerID, masterAddress)

	pollInterval := 2 * time.Second
	for {
		args := &shufflerpc.AssignTaskArgs{WorkerId: workerID}
		var reply shufflerpc.AssignTaskReply
		if err := client.Call("Master.AssignTask", args, &reply); err != nil {
			return fmt.Errorf("assign task rpc: %w", err)
		}
		if reply.Error != "" {
			return fmt.Errorf("master returned error: %s", reply.Error)
		}

		if reply.Task.TaskId == "" {
			time.Sleep(pollInterval)
			continue
		}

		taskType := "map"
		if reply.Task.Type == types.ReduceTask {
			taskType = "reduce"
		}
		log.Printf("[Worker %s] received %s task: %s", workerID, taskType, reply.Task.TaskId)

		locations, err := executeTask(&reply.Task, workerAddress)
		if err != nil {
			log.Printf("[Worker %s] task=%s failed: %v", workerID, reply.Task.TaskId, err)
			time.Sleep(pollInterval)
			continue
		}

		log.Printf("[Worker %s] task=%s completed", workerID, reply.Task.TaskId)

		reportArgs := &shufflerpc.ReportTaskCompletionArgs{
			TaskId:             reply.Task.TaskId,
			WorkerId:           workerID,
			PartitionLocations: locations,
		}
		var reportReply shufflerpc.ReportTaskCompletionReply
		if err := client.Call("Master.ReportTaskCompletion", reportArgs, &reportReply); err != nil {
			log.Printf("[Worker %s] report completion rpc failed: %v", workerID, err)
			time.Sleep(pollInterval)
			continue
		}
		if reportReply.Error != "" {
			log.Printf("[Worker %s] report completion error: %s", workerID, reportReply.Error)
			time.Sleep(pollInterval)
			continue
		}

		log.Printf("[Worker %s] task=%s reported to master successfully", workerID, reply.Task.TaskId)
	}
}

func executeTask(task *types.Task, workerAddress string) ([]types.PartitionLocationInfo, error) {
	switch task.Type {
	case types.MapTask:
		return executeMapTask(task, workerAddress)
	case types.ReduceTask:
		return nil, executeReduceTask(task)
	default:
		return nil, fmt.Errorf("unknown task type: %s", task.Type)
	}
}
