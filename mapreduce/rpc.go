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

type Phase string //stirng to keep track of which phase
const (
	Map_Phase    = "Map"    //mapping phase
	Reduce_Phase = "Reduce" //reducing phase
)

type ArgsGivemeTask struct { //worker sends an RPC to the coordinator with a unique worker value to let them know that they can work
	Uniqueworkervalue string //unique worker value
}

type ArgsDoneTask struct { //acknowledgement struct
	Gotit bool //Got it true or false
}
type ArgsWorkerSuccess struct { //struct used for success
	Filenames []string //filenames
	TaskName  int      //the task completed

	WorkerName string //which worker completed it

	Phase Phase //which phase
}

type ArgsGotit struct { //struct used for reply
	Gotit bool //got it if worker acknolowedges the rpc call
}

type ArgsTasktoComplete struct {
	TaskName                 int      //task id
	TasktoCompleteAssignment Phase    //mapping or reducing phase
	Filenames                []string //give the files
	Buckets                  int      //nreduce buckets
}

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}


