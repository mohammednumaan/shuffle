package types

import "time"

type TaskType string

const (
	MapTask    TaskType = "MapTask"
	ReduceTask TaskType = "ReduceTask"
)

type TaskState string

const (
	Idle       TaskState = "Idle"
	InProgress TaskState = "InProgress"
	Completed  TaskState = "Completed"
)

type WorkerState string

const (
	WorkerIdle        WorkerState = "Idle"
	WorkerBusy        WorkerState = "Busy"
	WorkerUnavailable WorkerState = "Unavailable"
)

type InputSplit struct {
	FilePath    string
	StartOffset int64
	EndOffset   int64
}

type PartitionLocation struct {
	MapTaskId     string
	WorkerAddress string
	FilePath      string
}

type PartitionLocationInfo struct {
	PartitionIdx  int
	FilePath      string
	WorkerAddress string
}

type Worker struct {
	WorkerId     string
	Address      string
	State        WorkerState
	LastPolledAt time.Time
}

type Task struct {
	TaskId string
	JobId  string
	Type   TaskType
	State  TaskState

	AssignedWorkerId string
	Split            *InputSplit
	InputDir         string
	OutputDir        string
	ReducerIdx       int
	NumReducers      int

	RetryCount         int
	MaxRetries         int
	PartitionLocations []*PartitionLocation
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
