package worker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"

	netrpc "net/rpc"

	shufflerpc "github.com/mohammednumaan/shuffle/internal/rpc"
	"github.com/mohammednumaan/shuffle/internal/types"
	"github.com/mohammednumaan/shuffle/internal/validation"
)

var (
	rpcClients   = make(map[string]*netrpc.Client)
	rpcClientsMu sync.Mutex
)

func executeReduceTask(task *types.Task) (string, error) {
	if err := validation.ValidateReduceTask(task); err != nil {
		return "", err
	}

	log.Printf("[Reducer %s] starting with %d partition locations", task.TaskId, len(task.PartitionLocations))

	grouped := make(map[string][]string)
	for _, loc := range task.PartitionLocations {
		log.Printf("[Reducer %s] fetching partition from %s", task.TaskId, loc.WorkerAddress)
		data, err := fetchPartition(loc)
		if err != nil {
			return loc.WorkerAddress, fmt.Errorf("fetch partition from worker %s: %w", loc.WorkerAddress, err)
		}

		if err := decodePartitionData(data, grouped); err != nil {
			return "", fmt.Errorf("decode partition data from worker %s: %w", loc.WorkerAddress, err)
		}

	}

	log.Printf("[Reducer %s] grouped into %d unique keys", task.TaskId, len(grouped))

	if err := os.MkdirAll(task.OutputDir, 0o755); err != nil {
		return "", fmt.Errorf("output dir %s: %w", task.OutputDir, err)
	}

	if reducerFn == nil {
		return "", fmt.Errorf("reducer function not registered")
	}

	return "", writeReducerOutput(task.OutputDir, task.ReducerIdx, grouped, reducerFn)

}

func fetchPartition(loc *types.PartitionLocation) ([]byte, error) {
	client, err := getRPCClient(loc.WorkerAddress)
	if err != nil {
		return nil, err
	}

	args := &shufflerpc.FetchPartitionArgs{
		FilePath: loc.FilePath,
	}

	var reply shufflerpc.FetchPartitionReply
	if err := client.Call("WorkerRPC.FetchPartition", args, &reply); err != nil {
		return nil, fmt.Errorf("fetch partition rpc call: %w", err)
	}
	if reply.Error != "" {
		return nil, fmt.Errorf("worker error: %s", reply.Error)
	}

	return reply.Data, nil
}

// this is to avoid opening a new tcp connection for every partition file
// since a reducer could be fetching multiple partition files from the same worker, we can reuse the same RPC client connection
func getRPCClient(addr string) (*netrpc.Client, error) {
	rpcClientsMu.Lock()
	defer rpcClientsMu.Unlock()

	if client, ok := rpcClients[addr]; ok {
		return client, nil
	}

	client, err := netrpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dialing worker at %s: %w", addr, err)
	}

	rpcClients[addr] = client
	return client, nil
}

func decodePartitionData(data []byte, grouped map[string][]string) error {

	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var record types.KeyValue[string, string]
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decoding partition data: %w", err)
		}

		grouped[record.Key] = append(grouped[record.Key], record.Value)
	}

	return nil
}

func writeReducerOutput(outputDir string, reducerIdx int, groupedData map[string][]string, reducer types.Reducer) error {
	outputPath := filepath.Join(outputDir, fmt.Sprintf("reducer-%d", reducerIdx))
	tmpOutputPath := fmt.Sprintf("%s.tmp", outputPath)
	log.Printf("[Reducer] writing output to %s (%d keys)", outputPath, len(groupedData))
	file, err := os.Create(tmpOutputPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}

	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)

	keys := make([]string, 0, len(groupedData))
	for key := range groupedData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		res, err := reducer.Reduce(key, groupedData[key])
		if err != nil {
			return fmt.Errorf("reducing key %s: %w", key, err)
		}
		if err := encoder.Encode(types.KeyValue[string, string]{Key: key, Value: res}); err != nil {
			return fmt.Errorf("encoding output: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flushing writer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing output file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing output file: %w", err)
	}
	if err := os.Rename(tmpOutputPath, outputPath); err != nil {
		return fmt.Errorf("atomic replace output file: %w", err)
	}

	return nil
}
