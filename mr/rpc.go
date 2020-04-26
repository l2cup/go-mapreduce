package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Args struct {
	JobId int
}

type Reply struct {
	ReduceNumber int
	JobId        int
	JobType      JobType
	FilePath     string
}

// Make a unix socket to connect master to workers
func masterSock() string {
	s := "mrmaster_sock_"
	s += strconv.Itoa(os.Getuid())
	return s
}
