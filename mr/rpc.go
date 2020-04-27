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

// Args is a struct containing rpc arugments.
type Args struct {
	JobId int
}

// Args is a struct containing rpc reply stuff.
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
