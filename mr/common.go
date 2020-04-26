package mr

/* JobType defines jobtype and current MapReduce phase */

type JobType string

const (
	MapJob    JobType = "Map"
	ReduceJob         = "Reduce"
)
