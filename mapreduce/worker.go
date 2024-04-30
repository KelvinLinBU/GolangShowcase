package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var worker_name string //unique worker name so we can use to id if worker dies on us
// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker_name = strconv.Itoa(rand.Intn(1000000000)) //random worker name
	//for assignment, err :=

	for { //for loop
		assignment := Ask_for_Work_Coordinator() //rpc call to the coordinator
		if assignment.TasktoCompleteAssignment == Map_Phase {
			if len(assignment.Filenames) > 0 { //if there are actually assignments
				assignmentid := assignment.TaskName //get the map task number to be used for naming intermediate files
				filename := assignment.Filenames[0] //take the first task
				file, err := os.Open(filename)      //open the file
				if err != nil {                     //error case
					log.Fatal("Couldn't open file\n")
				}
				content, err := io.ReadAll(file)                    //read the content
				intermediatekeys := mapf(filename, string(content)) //apply the mapf function to get []keyvalues
				intermediate_files := []string{}                    //intermediate file storage
				encoders := make(map[int]*json.Encoder)             //encoder from code in class
				for i := 0; i < assignment.Buckets; i++ {           //for # of assingment buckets
					tmpFile, err := ioutil.TempFile("", "map")
					if err != nil { //error case
						log.Fatal("Couldn't intermediate file\n")
					}
					intermediate_files = append(intermediate_files, tmpFile.Name()) //intermediate files appended with the temp files
					enc := json.NewEncoder(tmpFile)                                 //encoding
					encoders[i] = enc
				}
				for _, kv := range intermediatekeys { //for the key value pairs in intermediate keys
					reduce_num := ihash(kv.Key) % assignment.Buckets //find which reducer to send to
					encoders[reduce_num].Encode(&kv)                 //set the encoder
				}
				for i := 0; i < assignment.Buckets; i++ { //for the assignment buckets rename all the files once completely written
					mapOutputFileName := fmt.Sprintf("mr-%d-%d.txt", assignmentid, i) //
					os.Rename(intermediate_files[i], mapOutputFileName)               //rename with the mapoutputfilename
					intermediate_files[i] = mapOutputFileName                         //intermediate files index i set to mapoutputfilename
				}

				//rpc to coordintaror
				Success_Coordinator(assignmentid, intermediate_files, Map_Phase)
			}
		}
		if assignment.TasktoCompleteAssignment == Reduce_Phase { //if the phase is reduce
			files := assignment.Filenames    //assign files
			intermediate := []KeyValue{}     //initialize KeyValue slice
			for _, filename := range files { //in the files...
				filetoreduce, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Couldn't open file")
				}
				dec := json.NewDecoder(filetoreduce) //code from gitlab
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			//sort resulting key values
			sort.Sort(ByKey(intermediate))                              //taken from mrsequential                              //sort taken from mrsequential
			outputname := fmt.Sprintf("mr-out-%d", assignment.TaskName) //This is the final output name mr-out-X
			temp_file, _ := ioutil.TempFile("", "temp-")                //create a temporary file
			i := 0                                                      //code taken from mrsequentuial and modified to be used in this program
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				kva := reducef(intermediate[i].Key, values) //apply reducef

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(temp_file, "%v %v\n", intermediate[i].Key, kva)

				i = j
			}
			temp_file.Close()                       //close the temporary file
			os.Rename(temp_file.Name(), outputname) //this means the job is complete. Now you can rename it to the final file
			//RPC call to coordinator passing in the task name, files, and task phase.
			Success_Coordinator(assignment.TaskName, assignment.Filenames, assignment.TasktoCompleteAssignment)
		}
		//time.Sleep(3 * time.Second) //sleep for 3 seconds
	}
}

func Success_Coordinator(assignmentid int, name_of_files []string, phase Phase) error { //rpc call to coordinaotr letting them know that job is complete
	success := ArgsWorkerSuccess{WorkerName: worker_name, Filenames: name_of_files, Phase: phase, TaskName: assignmentid} //these are the values that I want to pass to the coordinator
	reply := ArgsGotit{}
	//got it struct
	acknowledge := call("Coordinator.Success_Handler", &success, &reply) //rpc call to success_handler
	if acknowledge == false {                                            //if fail or no acknowledgement
		log.Fatal("Death")
	}
	return nil
}

func Ask_for_Work_Coordinator() *ArgsTasktoComplete { //rpc for worker to ask for work
	request := ArgsGivemeTask{}                                                 //initialize
	request.Uniqueworkervalue = worker_name                                     //unique worker value from global worker name
	task_to_complete := ArgsTasktoComplete{}                                    //task to complete, with info given from coord
	call("Coordinator.Worker_Being_Assigned_Task", &request, &task_to_complete) //rpc call

	return &task_to_complete //return the task
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}


