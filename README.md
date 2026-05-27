# shuffle

a distributed mapreduce implementation in go, built for learning.

## what is mapreduce?

mapreduce is a programming model for processing large datasets in parallel across a cluster. the core idea is simple:

1. **map**: transform input data into intermediate key-value pairs
2. **shuffle**: group all values by key across all mappers
3. **reduce**: combine values for each key into a final result

## architecture

```mermaid
flowchart TD
    M["Master<br/>(orchestrator)"]
    
    NFS[("Shared NFS<br/>(Input + Intermediate)")]
    
    C["Chunk Builder"]
    
    subgraph MAP["Map Phase"]
        W1["Mapper Worker 1"]
        W2["Mapper Worker 2"]
        WN["Mapper Worker N"]
    end
    
    PARTS["Partitions 0..N<br/>(on NFS)"]
    
    subgraph RED["Reduce Phase"]
        R1["Reducer Worker 1"]
        R2["Reducer Worker 2"]
        RN["Reducer Worker N"]
    end
    
    OUT[("Final Output<br/>(on NFS)")]
    
    M <-->|read input files| NFS
    M -->|split input| C
    C -->|chunks| W1
    C -->|chunks| W2
    C -->|chunks| WN
    
    W1 -->|write mapped data| PARTS
    W2 -->|write mapped data| PARTS
    WN -->|write mapped data| PARTS
    
    PARTS <-->|read partitions| R1
    PARTS <-->|read partitions| R2
    PARTS <-->|read partitions| RN
    
    R1 -->|write result| OUT
    R2 -->|write result| OUT
    RN -->|write result| OUT
```


## intermediate file layout

each mapper creates one output directory with partition files for each reducer. each reducer reads its assigned partition from all mapper output directories, groups by key, and produces the final result.

**Layout:**
- Map output: `mapper-{id}/partition-{partition-id}`
- Reduce input: reads all `mapper-*/partition-{assigned-id}`
- Reduce output: merged results for the assigned partition

## task lifecycle

```mermaid
stateDiagram-v2
    [*] --> idle
    idle --> in_progress : assigned to worker
    in_progress --> completed : worker reports done
    in_progress --> failed : worker/k8s job fails
    failed --> idle : retry (backoff)
    completed --> [*]
```
