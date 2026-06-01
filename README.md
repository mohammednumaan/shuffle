# shuffle

a distributed mapreduce implementation in go, built for learning.

## what is mapreduce?

mapreduce is a programming model for processing large datasets in parallel across a cluster. the core idea is simple:

1. **map**: transform input data into intermediate key-value pairs
2. **shuffle**: group all values by key across all mappers
3. **reduce**: combine values for each key into a final result

the canonical example is **word count**: map splits each line into words (key=word, value="1"), shuffle groups by word, reduce sums the counts.

## architecture

the system has two components communicating over TCP RPC. given `N` machines, it creates `M = N` map tasks and `R = N` reduce tasks, matching the workers available in the cluster.

**master** — a single coordinator that:
- splits input files into `M` chunks and creates map tasks
- registers workers that join the cluster
- assigns map/reduce tasks to idle workers
- collects partition location metadata from completed mappers
- creates `R` reduce tasks once the map phase is fully done

**worker** — a task executor that:
- registers with the master on startup
- polls the master for available tasks (`AssignTask` RPC)
- runs map or reduce tasks
- reports task completion back to the master
- hosts an internal RPC server so reducers can fetch partition data over the shuffle network
