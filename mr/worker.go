package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// ihash is a hashing function used to reduce the task number
// for each KeyValue emitted by Map.
// Used as ihash(Key) % nReduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker starts a new worker which then queues for a job.
// If a worker doesn't get a job or a job failes it sleeps for 5 seconds.
// Workers will shutdown when the master shuts down automatically.
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	if call("Master.RegisterWorker", &struct{}{}, &struct{}{}) == false {
		return
	}

	for {
		if queueForJob(mapf, reducef) == false {
			time.Sleep(2 * time.Second)
			shutdown()
		}
	}
}

// queueForJob is a method used to ask the master for a job.
// It's arguments are the map and reduce function to be used for the given job.
// Returns a bool. False if there was an error, doesn't return the error.
func queueForJob(mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	args := Args{}
	reply := Reply{}

	ret := call("Master.RequestJob", &args, &reply)
	if ret == false {
		return ret
	}
	if reply.JobType == MapJob {
		if err := mapJob(&reply, mapf); err != nil {
			fmt.Printf("[Worker] queueForJob - map: %v \n", err.Error())
			return false
		}
	} else {
		if err := reduceJob(&reply, reducef); err != nil {
			fmt.Printf("[Worker] queueForJob - reduce: %v \n", err.Error())
			return false
		}
	}
	return finishJob(reply.JobId)

}

// mapJob is the map part of MapReduce. It takes the map function as an
// argument and a *Reply. It reads the given file and passes it's content
// to the map function. It sorts the output and writes it into intermediate
// files. Returns an error if something goes wrong, otherwise nil.
func mapJob(reply *Reply, mapf func(string, string) []KeyValue) error {

	content, err := ioutil.ReadFile(reply.FilePath)

	if err != nil {
		return err
	}

	intermediate := mapf(reply.FilePath, string(content))
	sort.Sort(ByKey(intermediate))
	var files []*os.File
	var encoders []*json.Encoder

	/* We write to temp files to assure crash protection */

	for i := 0; i < reply.ReduceNumber; i++ {
		file, err := ioutil.TempFile("./", fmt.Sprintf("mr-%v-%v-", reply.JobId, i))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(file)
		encoders = append(encoders, enc)
		files = append(files, file)
	}

	for _, kv := range intermediate {
		fileNum := ihash(kv.Key) % reply.ReduceNumber
		err := encoders[fileNum].Encode(kv)
		if err != nil {
			fmt.Printf("[Worker] Error Encoding : %v", err.Error())
			return err
		}
	}
	/* When we are done with writing we atomically rename files */
	for i := 0; i < reply.ReduceNumber; i++ {
		os.Rename(files[i].Name(), fmt.Sprintf("./mr-%v-%v", reply.JobId, i))
		files[i].Close()
	}

	return nil

}

// reduceJob is the reduce part of MapReduce. It takes the reduce function as an
// argument and a *Reply. It reads all map files for given reduce job id.
// sort(k, v) -> sorted(k, v) -> (k, (list(v))) -> reduce(k, (list(v)))
// Returns an error if something goes wrong, otherwise nil.
func reduceJob(reply *Reply, reducef func(string, []string) string) error {

	var files []*os.File
	var decoders []*json.Decoder

	var outputFile, err = ioutil.TempFile("./", fmt.Sprintf("mr-out-%v-", reply.JobId))
	if err != nil {
		return errors.New(fmt.Sprintf("Couldn't make output file for reduce job no. %v\n", reply.JobId))
	}

	for i := 0; ; i++ {
		file, err := os.Open(fmt.Sprintf("./mr-%v-%v", i, reply.JobId))
		if err != nil {
			break
		}
		dec := json.NewDecoder(file)
		files = append(files, file)
		decoders = append(decoders, dec)
	}

	var intermediate []KeyValue
	for i := 0; i < len(files); i++ {
		for {
			var kv KeyValue
			if err := decoders[i].Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	for _, file := range files {
		file.Close()
	}
	os.Rename(outputFile.Name(), fmt.Sprintf("./mr-out-%v", reply.JobId))
	outputFile.Close()

	return nil
}

// finishJob is used to notify the master that the worker calling is done with the task.
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

func shutdown() {
	if call("Master.Shutdown", &struct{}{}, &struct{}{}) == false {
		fmt.Printf("[Worker] Exiting gracefully\n")
		os.Exit(1)
	}
}
