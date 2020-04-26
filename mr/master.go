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

type Task struct {
	file    string
	jobType JobType
	jobId   int
	done    chan struct{}
}

type Tasks []Task

type Master struct {
	nReduce         int
	phase           JobType
	idleTasks       Tasks
	inProgressTasks Tasks
	inProgressCount int

	tasksMux sync.Mutex
	phaseMux sync.Mutex

	phaseCh chan struct{}
}

func (t Tasks) taskById(jobId int) (Task, error) {
	for _, task := range t {
		if task.jobId == jobId {
			return task, nil
		}
	}

	return Task{}, errors.New(fmt.Sprintf("taskById: No task with id %v", jobId))
}

func (m *Master) RequestJob(args *Args, reply *Reply) error {

	m.phaseMux.Lock()
	defer m.phaseMux.Unlock()

	switch m.phase {

	case MapJob:
		return m.startMapJob(args, reply)
	case ReduceJob:
		return m.startReduceJob(args, reply)
	}

	return nil

}

func (m *Master) startMapJob(args *Args, reply *Reply) error {

	m.tasksMux.Lock()
	defer m.tasksMux.Unlock()

	if len(m.idleTasks) == 0 {
		return errors.New("No more new tasks")
	}

	reply.ReduceNumber = m.nReduce

	taskNum := len(m.idleTasks) - 1

	reply.JobType = m.idleTasks[taskNum].jobType
	reply.JobId = m.idleTasks[taskNum].jobId

	m.inProgressTasks = append(m.inProgressTasks, m.idleTasks[taskNum])
	taskNum = len(m.inProgressTasks) - 1
	reply.FilePath = m.inProgressTasks[taskNum].file
	go m.timeout(m.inProgressTasks[taskNum].done, m.inProgressTasks[taskNum].jobId)
	m.inProgressCount++
	m.idleTasks = m.idleTasks[:len(m.idleTasks)-1]

	return nil
}

func (m *Master) startReduceJob(args *Args, reply *Reply) error {
	return errors.New("Not Implemented")
}

func (m *Master) JobDone(args *Args, reply *Reply) error {
	m.tasksMux.Lock()
	task, err := m.inProgressTasks.taskById(args.JobId)
	if err != nil {
		return errors.New(fmt.Sprintf("JobDone: Task with id %v doesn't exist.\n", args.JobId))
	}
	m.phaseCh <- struct{}{}
	task.done <- struct{}{}
	task.jobId = -1
	m.inProgressCount--
	m.tasksMux.Unlock()
	return nil
}

func (m *Master) timeout(done chan struct{}, jobId int) {
	select {
	case <-done:
		return
	case <-time.After(10 * time.Second):
		m.tasksMux.Lock()
		task, err := m.inProgressTasks.taskById(jobId)
		if err != nil {
			return
		}
		m.cleanup()
		m.idleTasks = append(m.idleTasks, task)
		m.inProgressCount--
		m.tasksMux.Unlock()
	}
}

func (m *Master) cleanup() {
	last := 0
	for index, task := range m.inProgressTasks {
		if task.jobId == -1 {
			last = index
		} else {
			break
		}
	}
	m.inProgressTasks = m.inProgressTasks[last:]
}

func (m *Master) changePhase(mapJobCount int) {
	for {
		select {
		case <-m.phaseCh:
			mapJobCount--
			if mapJobCount == 0 {
				m.phaseMux.Lock()
				m.tasksMux.Lock()
				m.phase = ReduceJob
				m.inProgressTasks = nil
				m.idleTasks = nil
				m.tasksMux.Unlock()
				m.phaseMux.Unlock()
				return
			}
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.

func (m *Master) Done() bool {

	m.phaseMux.Lock()
	m.tasksMux.Lock()
	defer m.phaseMux.Unlock()
	defer m.tasksMux.Unlock()
	if m.phase == ReduceJob && len(m.idleTasks) == 0 && m.inProgressCount == 0 {
		return true
	} else {
		return false
	}

}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{nReduce: nReduce, inProgressCount: 0, phase: MapJob, phaseCh: make(chan struct{})}
	for index, file := range files {
		m.idleTasks = append(m.idleTasks, Task{file: file, jobType: MapJob, jobId: index, done: make(chan struct{})})
	}
	go m.changePhase(len(m.idleTasks))

	m.serve()
	return &m
}
