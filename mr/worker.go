package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey is used for sorting by map key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		if queueForJob() == false {
			fmt.Printf("Sleeping for 3 secs.\n")
			time.Sleep(3 * time.Second)
			break
		}
	}
}

func queueForJob() bool {
	args := Args{}
	reply := Reply{}

	// send the RPC request, wait for the reply.
	ret := call("Master.RequestJob", &args, &reply)
	if ret == false {
		return ret
	}

	if reply.JobType == MapJob {
		mapJob()
	} else {
		reduceJob()
	}

	return finishJob(reply.JobId)

}

func mapJob() {
}

func reduceJob() {

}

func finishJob(jobId int) bool {
	args := Args{JobId: jobId}
	reply := Reply{}
	return call("Master.JobDone", &args, &reply)
}

// call sends an RPC request to the master, returns true if the request is ok,
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
