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

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args_Worker_Can_Work struct {
	//The worker can work if value is 1, if not 0
	Work_Yes_or_No int
}

type Args_Task_Assignment_to_Worker struct {
	//send back map task
	Maporreduce string
	Reply_path  string
	Nreduce     int
	Tasknumber  int
	Exit        int
}

type Args_Status struct {
	SuccessorFailure bool
	Path             string
	Map_task_num     int
}

type Args_Status_Reply struct {
	Gotit int
}

type Args_Done struct {
	Done bool
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


