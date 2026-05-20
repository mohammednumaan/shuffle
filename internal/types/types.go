package types

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
	OutputPath     string
}

type Emitter interface {
	Emit(key string, value string) error
}

type Mapper interface {
	Map(key string, value string, emit Emitter)
}
