package types

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
)

type TaskStatus string

const (
	Idle       TaskStatus = "idle"
	InProgress TaskStatus = "in-progress"
	Completed  TaskStatus = "completed"
	Failed     TaskStatus = "failed"
)

type InputSplit struct {
	FilePath    string
	StartOffset int64
	EndOffset   int64
}

type Task struct {
	Id             string
	Type           TaskType
	Status         TaskStatus
	Split          InputSplit
	AssignedWorker string
	OutputPath     string
}

type KeyValue[K, V any] struct {
	Key   K `json:"key"`
	Value V `json:"value"`
}

type MapFunc func(string, string) []KeyValue[string, string]

type ReduceFunc func(string, []string) (string, error)

type Mapper interface {
	Map(key string, value string) []KeyValue[string, string]
}

type Reducer interface {
	Reduce(key string, values []string) (string, error)
}
