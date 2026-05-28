package types

import "time"

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
	RetryCount     int
	RetryAfter     time.Time
	MaxRetries     int
}

type KeyValue[K, V any] struct {
	Key   K `json:"key"`
	Value V `json:"value"`
}

type Mapper interface {
	Map(key string, value string) []KeyValue[string, string]
}

type Reducer interface {
	Reduce(key string, values []string) (string, error)
}

type Config struct {
	Mode      string
	InputDir  string
	OutputDir string

	InputFile   string
	StartOffset int64
	EndOffset   int64

	NumMappers  int
	NumReducers int
	SplitSizeMB int64

	NfsPath    string
	Image      string
	Kubeconfig string

	Mapper     Mapper
	Reducer    Reducer
	ReducerIdx int
}

func (cfg *Config) RegisterFn(mapperFn Mapper, reducerFn Reducer) {
	cfg.Mapper = mapperFn
	cfg.Reducer = reducerFn
}
