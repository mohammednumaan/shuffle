package rpc

import (
	"github.com/mohammednumaan/shuffle/internal/types"
)

type RegisterWorkerArgs struct {
	WorkerId string
	Address  string
}

type RegisterWorkerReply struct {
	Error string
}

type AssignTaskArgs struct {
	WorkerId string
}

type AssignTaskReply struct {
	Task  types.Task
	Error string
}

type ReportTaskCompletionArgs struct {
	TaskId             string
	WorkerId           string
	PartitionLocations []types.PartitionLocationInfo
}

type ReportTaskCompletionReply struct {
	Error string
}

type FetchPartitionArgs struct {
	FilePath string
}

type FetchPartitionReply struct {
	Data  []byte
	Error string
}

type ReportTaskFailureArgs struct {
	TaskId           string
	WorkerId         string
	FailedWorkerAddr string
	Error            string
}

type ReportTaskFailureReply struct {
	Error string
}

type PingArgs struct{}
type PingReply struct{}
