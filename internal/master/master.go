package master

import (
	"fmt"
	"math"
	"net"
	netrpc "net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	shufflerpc "github.com/mohammednumaan/shuffle/internal/rpc"
	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/validation"
)

type Master struct {
	JobId              string
	MapTasks           []*types.Task
	ReduceTasks        []*types.Task
	Workers            map[string]*types.Worker
	PartitionLocations map[int][]*types.PartitionLocation
	InputDirectory     string
	OutputDirectory    string
	NumMappers         int
	NumReducers        int
	mu                 sync.Mutex
}

func Run(jobId, addr, inputDirectory, outputDirectory string, numMappers, numReducers int) error {
	master, err := NewMaster(jobId, addr, inputDirectory, outputDirectory, numMappers, numReducers)
	if err != nil {
		return fmt.Errorf("create master: %w", err)
	}

	splits, err := BuildInputSplitsForMappers(inputDirectory, numMappers)
	if err != nil {
		return fmt.Errorf("build input splits: %w", err)
	}

	CreateMapTasks(master, splits)

	return nil
}

func NewMaster(jobId, addr, inputDirectory, outputDirectory string, numMappers, numReducers int) (*Master, error) {
	if err := validation.ValidateMasterRuntime(inputDirectory, outputDirectory, numMappers, numReducers); err != nil {
		return nil, err
	}

	master := Master{
		JobId:              jobId,
		MapTasks:           []*types.Task{},
		ReduceTasks:        []*types.Task{},
		Workers:            make(map[string]*types.Worker),
		PartitionLocations: make(map[int][]*types.PartitionLocation),
		InputDirectory:     inputDirectory,
		OutputDirectory:    outputDirectory,
		NumMappers:         numMappers,
		NumReducers:        numReducers,
	}

	if err := netrpc.Register(&master); err != nil {
		return nil, fmt.Errorf("register master rpc: %w", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[RPC Server] Master is listening on %s\n", addr)
	go netrpc.Accept(listener)

	return &master, nil
}

func (m *Master) RegisterWorker(args *shufflerpc.RegisterWorkerArgs, reply *shufflerpc.RegisterWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validation.ValidateWorkerID(args.WorkerId); err != nil {
		reply.Error = err.Error()
		return nil
	}

	if worker, exists := m.Workers[args.WorkerId]; exists {
		worker.LastPolledAt = time.Now()
		worker.State = types.WorkerIdle
		reply.Error = ""
		return nil
	}

	m.Workers[args.WorkerId] = &types.Worker{
		WorkerId:     args.WorkerId,
		Address:      args.Address,
		State:        types.WorkerIdle,
		LastPolledAt: time.Now(),
	}

	reply.Error = ""
	return nil
}

func BuildInputSplitsForMappers(inputDir string, numMappers int) ([]types.InputSplit, error) {
	if err := validation.ValidateNumMappers(numMappers); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	type fileEntry struct {
		path string
		size int64
	}

	var files []fileEntry
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

		files = append(files, fileEntry{path: filePath, size: fileInfo.Size()})
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
	for _, f := range files {
		for start := int64(0); start < f.size; start += targetSplitSize {
			end := start + targetSplitSize
			if end > f.size {
				end = f.size
			}

			splits = append(splits, types.InputSplit{
				FilePath:    f.path,
				StartOffset: start,
				EndOffset:   end,
			})
		}
	}

	return splits, nil
}

func CreateMapTasks(master *Master, splits []types.InputSplit) {
	for i, split := range splits {
		task := &types.Task{
			TaskId:             fmt.Sprintf("task-%d", i),
			JobId:              master.JobId,
			Type:               types.MapTask,
			State:              types.Idle,
			Split:              &split,
			NumReducers:        master.NumReducers,
			RetryCount:         0,
			MaxRetries:         3,
			AssignedWorkerId:   "",
			PartitionLocations: nil,
		}
		master.MapTasks = append(master.MapTasks, task)
	}
}

func CreateReduceTasks(master *Master) {
	for i := 0; i < master.NumReducers; i++ {
		task := &types.Task{
			TaskId:             fmt.Sprintf("reduce-task-%d", i),
			JobId:              master.JobId,
			Type:               types.ReduceTask,
			State:              types.Idle,
			Split:              nil,
			ReducerIdx:         i,
			InputDir:           master.InputDirectory,
			OutputDir:          master.OutputDirectory,
			NumReducers:        master.NumReducers,
			RetryCount:         0,
			MaxRetries:         3,
			AssignedWorkerId:   "",
			PartitionLocations: master.PartitionLocations[i],
		}
		master.ReduceTasks = append(master.ReduceTasks, task)
	}
}

func (m *Master) AssignTask(args *shufflerpc.AssignTaskArgs, reply *shufflerpc.AssignTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, ok := m.Workers[args.WorkerId]; ok {
		worker.LastPolledAt = time.Now()
	}

	for _, task := range m.MapTasks {
		if task.State == types.Idle {
			task.State = types.InProgress
			task.AssignedWorkerId = args.WorkerId
			reply.Task = *task
			reply.Error = ""
			return nil
		}
	}

	for _, task := range m.ReduceTasks {
		if task.State == types.Idle {
			task.State = types.InProgress
			task.AssignedWorkerId = args.WorkerId
			reply.Task = *task
			reply.Error = ""
			return nil
		}
	}

	reply.Task = types.Task{}
	reply.Error = ""
	return nil
}

func (m *Master) ReportTaskCompletion(args *shufflerpc.ReportTaskCompletionArgs, reply *shufflerpc.ReportTaskCompletionReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, task := range m.MapTasks {
		if task.TaskId == args.TaskId {
			if task.AssignedWorkerId != args.WorkerId {
				reply.Error = fmt.Sprintf("task %s not assigned to worker %s", args.TaskId, args.WorkerId)
				return nil
			}
			task.State = types.Completed
			for _, loc := range args.PartitionLocations {
				m.PartitionLocations[loc.PartitionIdx] = append(
					m.PartitionLocations[loc.PartitionIdx],
					&types.PartitionLocation{
						MapTaskId:     task.TaskId,
						FilePath:      loc.FilePath,
						WorkerAddress: loc.WorkerAddress,
					},
				)
			}

			// if all the map tasks are complete
			// i can safely create the reduce tasks since
			// all map tasks are finished
			if m.CheckIfMapPhaseComplete() {
				CreateReduceTasks(m)
			}

			reply.Error = ""
			return nil
		}
	}

	for _, task := range m.ReduceTasks {
		if task.TaskId == args.TaskId {
			if task.AssignedWorkerId != args.WorkerId {
				reply.Error = fmt.Sprintf("task %s not assigned to worker %s", args.TaskId, args.WorkerId)
				return nil
			}
			task.State = types.Completed
			reply.Error = ""
			return nil
		}
	}

	reply.Error = fmt.Sprintf("task %s not found", args.TaskId)
	return nil
}

// no need to acquire lock here since we are calling this within
// the ReportTaskCompletion handler which already has the lock acquired
func (m *Master) CheckIfMapPhaseComplete() bool {
	for _, task := range m.MapTasks {
		if task.State != types.Completed {
			return false
		}
	}

	return true
}
