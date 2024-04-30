package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Workerstatus string //struct to keep track of a worker's status
const (
	DoingNothingReadyForTask = "DoingNothingReadyForTask" //wprker is ready to be assigned a task
	Working                  = "Working"                  //worker is currently working
	Done                     = "Done"                     //the task is done
	Fail                     = "Fail"                     //the task has failed and mnust be reassigned to a different worker
)

type Coordinator struct {
	tasks_path_names []string                      //task path names
	n_reduces        int                           //n reduce buckets
	mu               sync.Mutex                    //mutex
	mapping_tasks    map[int]*InformationaboutTask //mapping the tasks into a map of struct InformationaboutTask
	reducing_tasks   map[int]*InformationaboutTask //information about reduces
	job_completion   bool                          //make sure job is completed
	waitgroup        sync.WaitGroup                //waitgroup for timer

}

type InformationaboutTask struct {
	time_started    time.Time    //value to be used for in timer
	worker_name     string       //worker it is going to be assigned to
	status          Workerstatus //the status of the worker, check the struct for this
	filesintaskpath []string     //files in the assigned path
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Worker_Being_Assigned_Task(GivemeTask *ArgsGivemeTask, TasktoComplete *ArgsTasktoComplete) error {
	c.mu.Lock()                                   //lock coordinator
	defer c.mu.Unlock()                           //unlock coordinator
	for id, assignment := range c.mapping_tasks { //go through the map
		if assignment.status == "DoingNothingReadyForTask" { //if worker is idle
			assignment.status = "Working"                         //task is being executed
			assignment.worker_name = GivemeTask.Uniqueworkervalue //set unique worker value
			TasktoComplete.TaskName = id                          //set task id name
			TasktoComplete.Buckets = c.n_reduces                  //set n_reduces for buckets
			TasktoComplete.Filenames = assignment.filesintaskpath //set files for mapping
			TasktoComplete.TasktoCompleteAssignment = Map_Phase   //tell job is map job
			assignment.time_started = time.Now()                  //time started, added on to time for timeout
			return nil
		}
		if assignment.status == "Fail" { //if worker failed
			assignment.status = "Working"                         //task is being executed
			assignment.worker_name = GivemeTask.Uniqueworkervalue //set unique worker value
			TasktoComplete.TaskName = id                          //set task id name
			TasktoComplete.Buckets = c.n_reduces                  //set n_reduces for buckets
			TasktoComplete.Filenames = assignment.filesintaskpath //set files for mapping
			TasktoComplete.TasktoCompleteAssignment = Map_Phase   //tell job is map job
			assignment.time_started = time.Now()                  //time started, added on to time for timeout
			return nil
		}
	}
	variable := 0 //variable to keep track of whether map tasks are done yet

	for _, maps := range c.mapping_tasks { //go through coordinator map tasks left

		if maps.status != Done { //if they are not done...
			variable += 1 //then variable != 0

		}
	}
	if variable == 0 { //if no map tasks left, that means only reduces left
		for id, assignment := range c.reducing_tasks {

			if assignment.status == "DoingNothingReadyForTask" { //if worker is idle
				assignment.status = "Working"                          //task is being executed
				assignment.worker_name = GivemeTask.Uniqueworkervalue  //set unique worker value
				TasktoComplete.TaskName = id                           //set task id name
				TasktoComplete.Buckets = c.n_reduces                   //set n_reduces for buckets
				TasktoComplete.Filenames = assignment.filesintaskpath  //set files for mapping
				TasktoComplete.TasktoCompleteAssignment = Reduce_Phase //tell job is reduce job
				assignment.time_started = time.Now()                   //time started
				return nil
			}
			if assignment.status == "Fail" { //if worker failed
				assignment.status = "Working"                          //task is being executed
				assignment.worker_name = GivemeTask.Uniqueworkervalue  //set unique worker value
				TasktoComplete.TaskName = id                           //set task id name
				TasktoComplete.Buckets = c.n_reduces                   //set n_reduces for buckets
				TasktoComplete.Filenames = assignment.filesintaskpath  //set files for mapping
				TasktoComplete.TasktoCompleteAssignment = Reduce_Phase //tell job is reduce job
				assignment.time_started = time.Now()                   //time started
				return nil
			}
		}
	}
	return nil
}

// hmmmmm
func get_Reduce_Task_Name(filename string) int {

	re := regexp.MustCompile(`mr-(\d+)-(\d+)\.txt`)

	// Find submatches in the filename
	matches := re.FindStringSubmatch(filename)
	if len(matches) == 3 {
		// Convert the captured string (the reduce task ID) to an integer
		reduceTaskID, err := strconv.Atoi(matches[2])
		if err != nil {
			log.Panicf("error converting reduce task id to integer: %v\n", err)
		}
		return reduceTaskID
	}

	// If the format does not match, log an error and panic
	log.Panicf("couldn't parse reduce task id from filename %v\n", filename)
	return -1
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	variable := 0 //variable to keep track of whether map tasks are done yet
	c.mu.Lock()
	for _, maps := range c.mapping_tasks { //go through coordinator map tasks left
		if maps.status != Done { //if they are not done...
			variable += 1 //then variable != 0
		}
	}
	for _, reduces := range c.reducing_tasks { //go through coordinator reduce tasks left
		if reduces.status != Done { //if they are not done...
			variable += 1 //then variable != 0
		}
	}
	c.mu.Unlock()
	if variable == 0 { //if variable == 0 then that means there are no tasks to be done anymore
		return true
	}
	// Your code here.

	return ret
}

func (c *Coordinator) Success_Handler(args *ArgsWorkerSuccess, reply *ArgsGotit) error {
	if args.Phase == Map_Phase {
		information_from_map_completed := c.mapping_tasks[args.TaskName] //it was a success, then remove the job from the queue
		c.mu.Lock()
		if information_from_map_completed.status != Done { //if it is not done
			information_from_map_completed.status = Done //then mark it done
			for _, mapintermediate := range args.Filenames {
				reduceTaskID := get_Reduce_Task_Name(mapintermediate) //get the reduce tasks
				// if there is already a reduce task, append filename to that reduce task
				if reduceTask, ok := c.reducing_tasks[reduceTaskID]; ok { //if it is okay to append
					reduceTask.filesintaskpath = append(reduceTask.filesintaskpath, mapintermediate) //append
				} else {
					taskreduce := InformationaboutTask{}                   //else create another map task
					taskreduce.status = DoingNothingReadyForTask           //needs to be done
					taskreduce.filesintaskpath = []string{mapintermediate} //give the tasks back
					c.reducing_tasks[reduceTaskID] = &taskreduce
				}
			}
		}
		reply.Gotit = true

		c.mu.Unlock()

	}
	//if reduce phase
	if args.Phase == Reduce_Phase {
		reduce_name := args.TaskName                                       //get the task number
		information_from_reduce_completed := c.reducing_tasks[reduce_name] //information and task
		c.mu.Lock()                                                        //lock coordinator
		information_from_reduce_completed.status = Done                    //set this task to done
		c.mu.Unlock()                                                      //unlock coordinator
		reply.Gotit = true

	}
	return nil
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.n_reduces = nReduce                                  //nReduces
	c.tasks_path_names = files                             //set files
	c.mapping_tasks = make(map[int]*InformationaboutTask)  //initialize mapping map
	c.reducing_tasks = make(map[int]*InformationaboutTask) //intialize reducing map
	for index, entry := range c.tasks_path_names {         //populate the maptask maps
		maptask := InformationaboutTask{}           //information blank
		maptask.status = "DoingNothingReadyForTask" //set all tasks as being ready to be completed
		maptask.filesintaskpath = []string{entry}   //set files in task path
		c.mapping_tasks[index+1] = &maptask         //next
	}
	// Your code here.
	c.job_completion = false //job is not completed
	c.waitgroup.Add(1)       //add the waitgroup
	go c.Checkforfailures()  //start a gortouine that runs in the background every ten seconds to check for stragglers
	c.server()

	return &c
}

func (c *Coordinator) Checkforfailures() { //checking for failures
	for {
		c.mu.Lock()                   //lock
		if c.job_completion == true { //if the job is complete no need to check anymore
			break
		}
		time_assigned := time.Now()                   //time assigned is now
		for _, taskdetails := range c.mapping_tasks { //go through all the mapping tasks
			if taskdetails.status == Working { //if it is working, then we can check if it has exceeded ten seconds
				if time_assigned.Sub(taskdetails.time_started).Seconds() > (10 * time.Second).Seconds() {
					taskdetails.status = Fail //if it takes longer than 10 seconds mark as fail
				}
			}
		}
		//same logic for the reducing tasks
		for _, taskdetails := range c.reducing_tasks {
			if taskdetails.status == Working {
				if time_assigned.Sub(taskdetails.time_started).Seconds() > (10 * time.Second).Seconds() {
					taskdetails.status = Fail
				}
			}
		}
		c.mu.Unlock()                //unlock mutex
		time.Sleep(time.Second * 10) //sleep for 10 seconds and do it again
	}
	c.mu.Unlock()      //unlock
	c.waitgroup.Done() //waitgroup
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


