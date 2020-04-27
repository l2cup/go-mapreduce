package mr

import (
	"errors"
	"fmt"
	"log"

	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Job is used to hold a job/job for workers to do.
type Job struct {
	file    string
	jobType JobType
	jobId   int
	done    chan struct{}
}

// Jobs is used as a type to get a method for getting a job by it's jobId
type Jobs []Job

// Master is the main master struct.
// Master holds the current phase, idle jobs and jobs in progress,
// current worker count and nReduce param.
// Master also holds jobs and phase mutexes as well as the phase channel.
type Master struct {
	nReduce         int
	phase           JobType
	idleJobs        Jobs
	inProgressJobs  Jobs
	inProgressCount int
	workerCount     int
	done            bool

	jobsMux  sync.Mutex
	phaseMux sync.Mutex

	phaseCh chan struct{}
}

// jobById returns the job which has the passed id.
// If the job doesn't exist it returns an error.
// NOTE: NOT THREAD SAFE! DO NOT USE WITHOUT LOCKING THE jobS FIRST.
// IT DOES NOT LOCK jobS ITSELF.
func (t Jobs) jobById(jobId int) (Job, error) {
	for _, job := range t {
		if job.jobId == jobId {
			return job, nil
		}
	}

	return Job{}, errors.New(fmt.Sprintf("jobById: No job with id %v", jobId))
}

// RegisterWorker is an rpc method for workers to register themselves.
// If the master isn't done it increments the worker count. Thread safe.
func (m *Master) RegisterWorker(_ *struct{}, _ *struct{}) error {
	m.phaseMux.Lock()
	defer m.phaseMux.Unlock()
	if m.done == true {
		return errors.New("No more jobs, can't register worker.\n")
	}
	m.workerCount++
	return nil
}

// RequestJob is an rpc method for workers to request a job.
// Returns an appropriate error if there aren't any jobs available.
func (m *Master) RequestJob(args *Args, reply *Reply) error {

	m.phaseMux.Lock()
	defer m.phaseMux.Unlock()

	return m.startJob(args, reply)

}

// startJob is the internal method that RequestJob calls.
// It either starts a Map or a Reduce job. If there aren't any jobs available
// it returns an error. Thread safe.
func (m *Master) startJob(args *Args, reply *Reply) error {

	m.jobsMux.Lock()
	defer m.jobsMux.Unlock()

	if len(m.idleJobs) == 0 {
		return errors.New("[Master] -> [Worker] No more new jobs, sleep for 2 seconds.")
	}

	reply.ReduceNumber = m.nReduce

	jobNum := len(m.idleJobs) - 1

	reply.JobType = m.idleJobs[jobNum].jobType
	reply.JobId = m.idleJobs[jobNum].jobId

	m.inProgressJobs = append(m.inProgressJobs, m.idleJobs[jobNum])
	jobNum = len(m.inProgressJobs) - 1
	reply.FilePath = m.inProgressJobs[jobNum].file
	go m.timeout(m.inProgressJobs[jobNum].done, m.inProgressJobs[jobNum].jobId)
	m.inProgressCount++
	m.idleJobs = m.idleJobs[:len(m.idleJobs)-1]

	return nil
}

// JobDone is a rpc method which workers call when they are finished with the given job.
// It sends a struct{} through a channel to notify the timeout method that it should return
func (m *Master) JobDone(args *Args, reply *Reply) error {
	m.jobsMux.Lock()
	job, err := m.inProgressJobs.jobById(args.JobId)
	if err != nil {
		return errors.New(fmt.Sprintf("JobDone: Job with id %v doesn't exist.\n", args.JobId))
	}

	job.done <- struct{}{}

	// If the phase is reduce, no one will listen to the channel,
	// so this will block on here forever.
	m.phaseMux.Lock()
	if m.phase == MapJob {
		m.phaseCh <- struct{}{}
	}
	m.phaseMux.Unlock()

	job.jobId = -1
	m.inProgressCount--
	m.jobsMux.Unlock()
	return nil
}

// timeout is called every time a worker gets a job. It holds the done channel and the jobId
// and if there isn't a message through the channel in 10 seconds it will put the job back
// in idleJobs. It also assumes that the worker crashed and reduces worker count.
func (m *Master) timeout(done chan struct{}, jobId int) {
	select {
	case <-done:
		return
	case <-time.After(10 * time.Second):
		m.jobsMux.Lock()
		job, err := m.inProgressJobs.jobById(jobId)
		if err != nil {
			return
		}
		fmt.Printf("In timeout for jobId %v, getting job back to queue.\n", jobId)
		m.cleanup()
		m.idleJobs = append(m.idleJobs, job)
		m.inProgressCount--
		m.workerCount--
		m.jobsMux.Unlock()
	}
}

// cleanup removes done jobs from inProgressJobs.
// It is only called by timeout when a worker crashes.
func (m *Master) cleanup() {
	last := 0
	for index, job := range m.inProgressJobs {
		if job.jobId == -1 {
			last = index
		} else {
			break
		}
	}
	m.inProgressJobs = m.inProgressJobs[last:]
}

// changePhase is used to countdown the mapJobCount and change the phase
// to reduce phase.
func (m *Master) changePhase(mapJobCount int) {
	for {
		_ = <-m.phaseCh
		mapJobCount--
		if mapJobCount == 0 {
			m.phaseMux.Lock()
			m.jobsMux.Lock()
			m.initiateReducePhase()
			m.jobsMux.Unlock()
			m.phaseMux.Unlock()
			return
		}

	}
}

// initiateReducePhase cleans job slices and creates nReduce reduce jobs.
func (m *Master) initiateReducePhase() {
	m.phase = ReduceJob

	for i := 0; i < m.nReduce; i++ {
		m.idleJobs = append(m.idleJobs, Job{jobType: ReduceJob, jobId: i, done: make(chan struct{})})
	}
	m.inProgressJobs = nil
	m.inProgressCount = 0

	fmt.Printf("[Master] Initiated reduce phase. Workers will soon start working.\n")
}

// serve starts a thread that listens for RPCs from worker.go
func (m *Master) serve() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done checks if the entire job is finished. Returns a bool indicating that.
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {

	m.phaseMux.Lock()
	m.jobsMux.Lock()
	defer m.phaseMux.Unlock()
	defer m.jobsMux.Unlock()
	if m.phase == ReduceJob && len(m.idleJobs) == 0 && m.inProgressCount == 0 {
		m.done = true
	}

	if m.done == true && m.workerCount == 0 {
		return true
	} else {
		return false
	}

}

// Shutdown is a rpc method for workers to ask if they should shut down.
// It returns an error if they should shutdown to act the same as when the
// rpc failes so the workers always shutdown gracefully.
func (m *Master) Shutdown(_ *struct{}, _ *struct{}) error {
	m.phaseMux.Lock()
	defer m.phaseMux.Unlock()
	if m.done == true {
		m.workerCount--
		return errors.New("[Master] Shutdown emitted.")
	}
	return nil
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce jobs to use.
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{nReduce: nReduce, inProgressCount: 0, phase: MapJob, phaseCh: make(chan struct{})}
	for index, file := range files {
		m.idleJobs = append(m.idleJobs, Job{file: file, jobType: MapJob, jobId: index, done: make(chan struct{})})
	}
	go m.changePhase(len(m.idleJobs))

	m.serve()
	return &m
}
