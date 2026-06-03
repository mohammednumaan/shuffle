package master

import (
	"fmt"
	"log"
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

const (
	pingInterval = 5 * time.Second
	pingTimeout  = 2 * time.Second
)

type Master struct {
	JobId              string
	MapTasks           []*types.Task
	ReduceTasks        []*types.Task
	Workers            map[string]*types.Worker
	PartitionLocations map[int][]*types.PartitionLocation
	JobCompleted       bool
	InputDirectory     string
	OutputDirectory    string
	NumMappers         int
	NumReducers        int
	mu                 sync.Mutex
	Done               chan struct{}
}

func Run(jobId, addr, inputDirectory, outputDirectory string, numMachines int) error {
	master, err := NewMaster(jobId, addr, inputDirectory, outputDirectory, numMachines)
	if err != nil {
		return fmt.Errorf("create master: %w", err)
	}

	splits, err := BuildInputSplitsForMappers(inputDirectory, master.NumMappers)
	if err != nil {
		return fmt.Errorf("build input splits: %w", err)
	}

	CreateMapTasks(master, splits)

	<-master.Done
	log.Printf("[Master] job %s complete, shutting down", jobId)
	return nil
}

func NewMaster(jobId, addr, inputDirectory, outputDirectory string, numMachines int) (*Master, error) {
	if err := validation.ValidateMasterRuntime(inputDirectory, outputDirectory, numMachines); err != nil {
		return nil, err
	}

	numMappers := numMachines
	numReducers := numMachines

	master := Master{
		JobId:              jobId,
		MapTasks:           []*types.Task{},
		ReduceTasks:        []*types.Task{},
		Workers:            make(map[string]*types.Worker),
		PartitionLocations: make(map[int][]*types.PartitionLocation),
		JobCompleted:       false,
		InputDirectory:     inputDirectory,
		OutputDirectory:    outputDirectory,
		NumMappers:         numMappers,
		NumReducers:        numReducers,
		Done:               make(chan struct{}),
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

	go master.runWorkerHealthChecks()
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
		log.Printf("[Master] worker re-registered: id=%s addr=%s", args.WorkerId, args.Address)
		reply.Error = ""
		return nil
	}

	m.Workers[args.WorkerId] = &types.Worker{
		WorkerId:     args.WorkerId,
		Address:      args.Address,
		State:        types.WorkerIdle,
		LastPolledAt: time.Now(),
	}

	log.Printf("[Master] worker registered: id=%s addr=%s (total=%d)", args.WorkerId, args.Address, len(m.Workers))
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
			AssignedWorkerId:   "",
			PartitionLocations: nil,
		}
		master.MapTasks = append(master.MapTasks, task)
	}
}

func CreateReduceTasks(master *Master) {
	if len(master.ReduceTasks) > 0 {
		for _, task := range master.ReduceTasks {
			if task.State != types.Idle {
				continue
			}
			task.PartitionLocations = master.PartitionLocations[task.ReducerIdx]
		}
		log.Printf("[Master] refreshed %d reduce tasks with latest partition locations", len(master.ReduceTasks))
		return
	}

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
			AssignedWorkerId:   "",
			PartitionLocations: master.PartitionLocations[i],
		}
		master.ReduceTasks = append(master.ReduceTasks, task)
	}
	log.Printf("[Master] created %d reduce tasks", len(master.ReduceTasks))
}

func (m *Master) AssignTask(args *shufflerpc.AssignTaskArgs, reply *shufflerpc.AssignTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.JobCompleted {
		reply.Task = types.Task{}
		reply.Error = ""
		return nil
	}

	worker, ok := m.Workers[args.WorkerId]
	if !ok {
		reply.Error = fmt.Sprintf("worker %s not registered", args.WorkerId)
		return nil
	}
	if worker.State == types.WorkerUnavailable {
		reply.Error = "worker marked unavailable"
		return nil
	}
	worker.LastPolledAt = time.Now()

	for _, task := range m.MapTasks {
		if task.State != types.Idle {
			continue
		}
		task.State = types.InProgress
		task.AssignedWorkerId = args.WorkerId
		reply.Task = *task
		log.Printf("[Master] assigned map task: %s -> worker=%s", task.TaskId, args.WorkerId)
		reply.Error = ""
		return nil
	}

	for _, task := range m.ReduceTasks {
		if task.State != types.Idle {
			continue
		}
		task.State = types.InProgress
		task.AssignedWorkerId = args.WorkerId
		reply.Task = *task
		reply.Task.PartitionLocations = task.PartitionLocations
		log.Printf("[Master] assigned reduce task: %s -> worker=%s", task.TaskId, args.WorkerId)
		reply.Error = ""
		return nil
	}

	reply.Task = types.Task{}
	reply.Error = ""
	return nil
}

func (m *Master) ReportTaskCompletion(args *shufflerpc.ReportTaskCompletionArgs, reply *shufflerpc.ReportTaskCompletionReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, ok := m.Workers[args.WorkerId]; ok && worker.State == types.WorkerUnavailable {
		reply.Error = "worker marked unavailable"
		return nil
	}

	for _, task := range m.MapTasks {
		if task.TaskId == args.TaskId {
			if task.State == types.Completed {
				log.Printf("[Master] ignoring duplicate completion for completed map task: %s worker=%s", task.TaskId, args.WorkerId)
				reply.Error = ""
				return nil
			}
			if task.State != types.InProgress {
				reply.Error = fmt.Sprintf("task %s not in progress", args.TaskId)
				return nil
			}
			if task.AssignedWorkerId != args.WorkerId {
				reply.Error = fmt.Sprintf("task %s not assigned to worker %s", args.TaskId, args.WorkerId)
				return nil
			}
			task.State = types.Completed
			for _, loc := range args.PartitionLocations {
				m.upsertPartitionLocation(
					loc.PartitionIdx,
					&types.PartitionLocation{
						MapTaskId:     task.TaskId,
						FilePath:      loc.FilePath,
						WorkerAddress: loc.WorkerAddress,
					},
				)
			}

			log.Printf("[Master] map task completed: %s worker=%s partitions=%d", task.TaskId, args.WorkerId, len(args.PartitionLocations))

			if m.CheckIfMapPhaseComplete() {
				log.Printf("[Master] ALL MAP TASKS COMPLETE — creating reduce tasks")
				CreateReduceTasks(m)
			}

			reply.Error = ""
			return nil
		}
	}

	for _, task := range m.ReduceTasks {
		if task.TaskId == args.TaskId {
			if task.State == types.Completed {
				log.Printf("[Master] ignoring duplicate completion for completed reduce task: %s worker=%s", task.TaskId, args.WorkerId)
				reply.Error = ""
				return nil
			}
			if task.State != types.InProgress {
				reply.Error = fmt.Sprintf("task %s not in progress", args.TaskId)
				return nil
			}
			if task.AssignedWorkerId != args.WorkerId {
				reply.Error = fmt.Sprintf("task %s not assigned to worker %s", args.TaskId, args.WorkerId)
				return nil
			}
			task.State = types.Completed
			completed := m.countCompletedReduceTasks()
			log.Printf("[Master] reduce task completed: %s worker=%s (%d/%d)", task.TaskId, args.WorkerId, completed, len(m.ReduceTasks))
			if len(m.ReduceTasks) > 0 && completed == len(m.ReduceTasks) {
				m.JobCompleted = true
				log.Printf("[Master] ALL REDUCE TASKS COMPLETE")
				close(m.Done)
			}
			reply.Error = ""
			return nil
		}
	}

	reply.Error = fmt.Sprintf("task %s not found", args.TaskId)
	return nil
}

func (m *Master) ReportTaskFailure(args *shufflerpc.ReportTaskFailureArgs, reply *shufflerpc.ReportTaskFailureReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.findTask(args.TaskId)
	if task == nil {
		reply.Error = fmt.Sprintf("task %s not found", args.TaskId)
		return nil
	}

	log.Printf("[Master] task %s failed: worker=%s failedWorker=%s error=%s", args.TaskId, args.WorkerId, args.FailedWorkerAddr, args.Error)

	m.resetTasks(args.WorkerId, task)

	if args.FailedWorkerAddr != "" {
		affectedMapTasks, affectedPartitions := m.cleanupPartitionLocations(args.FailedWorkerAddr)
		if m.requeueCompletedMapTasks(affectedMapTasks) {
			m.resetAffectedReduceTasks(affectedPartitions)
		}
		CreateReduceTasks(m)
	}
	reply.Error = ""
	return nil
}

func (m *Master) findTask(taskId string) *types.Task {
	for _, t := range m.MapTasks {
		if t.TaskId == taskId {
			return t
		}
	}
	for _, t := range m.ReduceTasks {
		if t.TaskId == taskId {
			return t
		}
	}
	return nil
}

func (m *Master) CheckIfMapPhaseComplete() bool {
	for _, task := range m.MapTasks {
		if task.State != types.Completed {
			return false
		}
	}

	return true
}

func (m *Master) countCompletedReduceTasks() int {
	completed := 0
	for _, task := range m.ReduceTasks {
		if task.State == types.Completed {
			completed++
		}
	}
	return completed
}

func (m *Master) upsertPartitionLocation(partitionIdx int, location *types.PartitionLocation) {
	locs := m.PartitionLocations[partitionIdx]
	filtered := make([]*types.PartitionLocation, 0, len(locs)+1)
	for _, loc := range locs {
		if loc.MapTaskId == location.MapTaskId {
			continue
		}
		filtered = append(filtered, loc)
	}
	filtered = append(filtered, location)
	m.PartitionLocations[partitionIdx] = filtered
}

func (m *Master) runWorkerHealthChecks() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		m.pingAllWorkers()
	}
}

func (m *Master) pingAllWorkers() {
	type snap struct{ id, addr string }

	// the reason i do this is to avoid holding the lock
	// while doing network io
	m.mu.Lock()
	targets := make([]snap, 0, len(m.Workers))
	for id, w := range m.Workers {
		targets = append(targets, snap{id: id, addr: w.Address})
	}
	m.mu.Unlock()

	for _, t := range targets {
		if err := m.pingWorker(t.addr); err != nil {
			m.handleWorkerFailure(t.id)
			continue
		}
		m.markWorkerAlive(t.id)
	}
}

// afaik there is no way to "kill" an RPC call
// so i found this: https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism
// to implement timeouts patterns
func (m *Master) pingWorker(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, pingTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(pingTimeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	client := netrpc.NewClient(conn)
	defer client.Close()

	var reply shufflerpc.PingReply
	if err := client.Call("WorkerRPC.Ping", &shufflerpc.PingArgs{}, &reply); err != nil {
		return err
	}
	return nil
}

// nothing fancy, just resets the task states
// so that it can be resheduled
func (m *Master) resetTasks(workerId string, task *types.Task) {
	if task.AssignedWorkerId != workerId || task.State != types.InProgress {
		return
	}
	task.State = types.Idle
	task.AssignedWorkerId = ""
	log.Printf("[Master] reset task %s to idle due to worker %s failure", task.TaskId, workerId)
}

// again, just resets the worker states
func (m *Master) markWorkerAlive(workerId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, ok := m.Workers[workerId]
	if !ok {
		return
	}
	if w.State == types.WorkerUnavailable {
		log.Printf("[Master] worker %s at %s is responsive again, marking idle", workerId, w.Address)
		w.State = types.WorkerIdle
	}
}

// handleWorkerFailure marks the worker Unavailable, resets all of its
// in-flight tasks to Idle so they can be reassigned, and evicts any
// partition locations that point at the dead worker
func (m *Master) handleWorkerFailure(workerId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.JobCompleted {
		return
	}

	w, ok := m.Workers[workerId]
	if !ok || w.State == types.WorkerUnavailable {
		return
	}
	log.Printf("[Master] worker %s at %s is unresponsive, marking unavailable", workerId, w.Address)
	w.State = types.WorkerUnavailable

	for _, task := range m.MapTasks {
		m.resetTasks(workerId, task)
	}
	for _, task := range m.ReduceTasks {
		m.resetTasks(workerId, task)
	}
	affectedMapTasks, affectedPartitions := m.cleanupPartitionLocations(w.Address)
	if m.requeueCompletedMapTasks(affectedMapTasks) {
		m.resetAffectedReduceTasks(affectedPartitions)
	}
}

// cleanupPartitionLocations drops any PartitionLocation whose WorkerAddress
// matches the dead worker.
func (m *Master) cleanupPartitionLocations(workerAddress string) (map[string]struct{}, map[int]struct{}) {
	affectedMapTasks := make(map[string]struct{})
	affectedPartitions := make(map[int]struct{})

	for idx, locs := range m.PartitionLocations {
		filtered := make([]*types.PartitionLocation, 0, len(locs))
		for _, loc := range locs {
			if loc.WorkerAddress == workerAddress {
				log.Printf("[Master] evicting partition %s for dead worker %s", loc.MapTaskId, workerAddress)
				affectedMapTasks[loc.MapTaskId] = struct{}{}
				affectedPartitions[idx] = struct{}{}
				continue
			}
			filtered = append(filtered, loc)
		}
		if len(filtered) == 0 {
			delete(m.PartitionLocations, idx)
		} else {
			m.PartitionLocations[idx] = filtered
		}
	}

	return affectedMapTasks, affectedPartitions
}

func (m *Master) requeueCompletedMapTasks(taskIDs map[string]struct{}) bool {
	if len(taskIDs) == 0 {
		return false
	}

	requeued := false
	for _, task := range m.MapTasks {
		if _, affected := taskIDs[task.TaskId]; !affected {
			continue
		}

		if task.State == types.Completed {
			task.State = types.Idle
			task.AssignedWorkerId = ""
			log.Printf("[Master] requeueing completed map task %s after partition loss", task.TaskId)
			requeued = true
		}
	}

	return requeued
}

func (m *Master) resetAffectedReduceTasks(partitions map[int]struct{}) {
	if len(m.ReduceTasks) == 0 || len(partitions) == 0 {
		return
	}

	for _, task := range m.ReduceTasks {
		if _, ok := partitions[task.ReducerIdx]; !ok {
			continue
		}

		if task.State == types.Completed || task.State == types.InProgress {
			log.Printf("[Master] resetting reduce task %s (partition=%d) after partition loss", task.TaskId, task.ReducerIdx)
			task.State = types.Idle
			task.AssignedWorkerId = ""
		}
	}
}
