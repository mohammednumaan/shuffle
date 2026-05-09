# phase.md
this file contains the phase-phase breakdown of how i'm going to implement `map-reduce` in `go`. each phase will have targets and the phases will build on top of each other. the final phase will be the complete implementation of `map-reduce` in `go`.

# important assumptions:
- as per the paper, there can be many different implementations of map-reduce. for example, one implementation may suit large NUMA processors, while others are suited for small, shared-memory machines. this implementation will be designed for a large clusters of commodity machines (which aligns with the original paper).

- a cluster may contains hundreds or thousands of machines, so machine failures are common. the implementation will need to be fault-tolerant.
- storage is provided by disks, which is directly attached to each machines. 
- users submit jobs and each job will be scheduled to run by schedulers. the implementation will need to support job scheduling and management.

# requirements:

from the `word count` example in the paper, we can derive the following requirements for the implementation:
- the user imports the map-reduce library and defines a `map` and `reduce` function.
- the user submits a job to the map-reduce system, which includes the input data and the `map` and `reduce` functions.

- the job will get distributed across multiple machines. these machines must have access to the input data.
- the output of mappers must be available to the reducers.
- finally, the output of the reducers must be collected and returned to the user.

# infrastructure:
i need to answer the following questions about the infrastructure:
- how will each machine access the input data?
- how do i distribute the map and reduce tasks across the machines?

to answer the first question:
- i can use a file system that is shared across all machines. to keep this implementation simple, i can use NFS (Network File System) to share the input data across all machines. this way, each machine can read the input data directly from the shared file system.

to answer the second question:
- i need a way to scale machines, manage the machines and execute tasks on the machines. this is very similar to what a cluster orchestration system like `kubernetes` does. to keep this implementation simple, i can use `kubernetes` to manage the machines and execute tasks on the machines. i will use `minikube` to set up a local cluster.
