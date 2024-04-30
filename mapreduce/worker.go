package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
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

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task_struct := Call_Coordinator() //send rpc to the coordinator and set task variable to the replied TaskAssignment. This is a struct
		if task_struct.Exit == 1 {
			fmt.Println("Worker exit")
			os.Exit(1)
		}
		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()

		filename := task_struct.Reply_path //extract the path name
		nReduce := task_struct.Nreduce
		maporreduce := task_struct.Maporreduce
		mapTaskNumber := task_struct.Tasknumber

		switch {
		case maporreduce == "done":
			os.Exit(1)
		case maporreduce == "wait": //If the worker is told to wait
			time.Sleep(5 * time.Second) //wait for 5 seconds and then ping the coordinator to check if status updates
			fmt.Println("slept for 5 seconds")
			//RPC Call?
		case maporreduce == "map": //if this is map task
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v: %v", filename, err)
			}
			defer file.Close()

			// Read the entire content of the file
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v: %v", filename, err)
			}

			// Execute the map function to get key-value pairs
			keyValues := mapf(filename, string(content))

			// Initialize encoders and temporary files for each reduce task
			encoders := make([]*json.Encoder, nReduce)
			tempFiles := make([]*os.File, nReduce)
			for i := 0; i < nReduce; i++ {
				intermediateFileName := fmt.Sprintf("tempmr-%d-%d.json", mapTaskNumber, i+1)
				outFile, err := os.Create(intermediateFileName)
				if err != nil {
					log.Fatalf("error creating intermediate file: %v", err)
				}
				// Note: Consider removing defer here and explicitly closing files later for better control
				encoders[i] = json.NewEncoder(outFile)
				tempFiles[i] = outFile
			}

			// Distribute key-value pairs to the appropriate reducer and encode
			for _, kv := range keyValues {
				reduceTaskNumber := ihash(kv.Key) % nReduce
				if err := encoders[reduceTaskNumber].Encode(kv); err != nil {
					log.Fatalf("error encoding key-value pair: %v", err)
				}
			}

			// Explicitly close all temporary files to ensure data is flushed
			for _, file := range tempFiles {
				if err := file.Close(); err != nil {
					log.Printf("error closing file: %v", err)
					// Handle error, possibly continue to attempt to close other files
				}
			}

			// Rename temporary files to their final names
			for i := 0; i < nReduce; i++ {
				tempFileName := fmt.Sprintf("tempmr-%d-%d.json", mapTaskNumber, i+1)
				finalFileName := fmt.Sprintf("mr-%d-%d.json", mapTaskNumber, i+1)
				if err := os.Rename(tempFileName, finalFileName); err != nil {
					log.Fatalf("error renaming intermediate file: %v", err)
				}
			}

			Status_Coordinator(true, filename, mapTaskNumber)
		case maporreduce == "reduce":

			//dirPath := "/Users/kelvin/CS350 Mega Folder/Assignment3NewNew/starter/mr-main"
			reduceTaskNum := mapTaskNumber // Example reduce task number, adjust based on your actual task numbering

			// Same process to decode KeyValue pairs and group them by key
			// Assume `groupedValues` is obtained as before

			// Path to the output file for this specific reduce task
			outputFileName := fmt.Sprintf("tempr-out-%dtemp", reduceTaskNum)
			//outputPath := filepath.Join(dirPath, outputFileName)
			outputFile, err := os.Create(outputFileName)
			if err != nil {
				log.Fatalf("Failed to create or truncate the file: %v", err)
			}
			// Create and open the output file

			input := filename

			// Regular expression to find "reduce-task-X" where "X" is some number
			re := regexp.MustCompile(`reduce-task-\d+`)

			// Find all matches in the input string
			match := re.FindAllString(input, -1)

			nice := match[0]
			//need to define better what filename is, it is inaccurate
			reducebucketdir, err := os.ReadDir(nice) //read bucket reduce directory
			if err != nil {
				log.Fatalf("Failed to read directory: %v", err)
			}
			//for every file in reducebucketfile
			for _, entry := range reducebucketdir { //for every file in directory
				if entry.IsDir() {
					continue
				}
				//probably go through and
				file, err := os.Open(entry.Name()) //open the file
				if err != nil {
					log.Fatalf("cannot open %v: %v", entry.Name(), err)
				}

				kva := []KeyValue{} //key value pairs slices
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				// Combine all values of the same keys into a map where the key is string and value is []string
				intermediate := kva
				sort.Sort(ByKey(intermediate))
				outputFileName := fmt.Sprintf("tempmr-out-%d", reduceTaskNum)
				ofile, _ := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				/*				realoutputFileName := fmt.Sprintf("mr-out-%d", reduceTaskNum)
								for key, values := range combined {
									reducedResult := reducef(key, values)
									_, err := fmt.Fprintf(outputFile, "%v %v\n", key, reducedResult)
									if err != nil {
										log.Printf("Failed to write to output file: %v", err)
										// Handle error, maybe break or continue based on your error policy
									}
								}*/

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				inputFile := outputFileName
				realoutputFileName := fmt.Sprintf("mr-out-%d", reduceTaskNum)
				//realoutputPath := filepath.Join(dirPath, realoutputFileName)

				// Open the input file
				oldfile, err := os.Open(inputFile)
				if err != nil {
					panic(err)
				}
				defer file.Close()

				// Create a map to hold the combined values for each key
				keyIntMap := make(map[string]int)       // Map for integer aggregations
				keyStringMap := make(map[string]string) // Map for string concatenations

				scanner := bufio.NewScanner(oldfile)
				for scanner.Scan() {
					line := scanner.Text()
					parts := strings.SplitN(line, " ", 2)
					if len(parts) < 2 {
						fmt.Println("Invalid line:", line) //Theres an issue here
						continue
					}
					key := parts[0]
					valueStr := parts[1]

					// Attempt to convert the string to an integer
					value, err := strconv.Atoi(valueStr)
					if err == nil { // It's an integer
						if existingValue, exists := keyIntMap[key]; exists {
							keyIntMap[key] = existingValue + value
						} else {
							keyIntMap[key] = value
						}
					} else { // It's not an integer, handle as string
						if existingValue, exists := keyStringMap[key]; exists {
							keyStringMap[key] = existingValue + "," + valueStr // Concatenate with a separator if needed
						} else {
							keyStringMap[key] = valueStr
						}
					}
				}

				if err := scanner.Err(); err != nil {
					panic(err)
				}

				// Create or overwrite the output file
				outFile, err := os.Create(realoutputFileName)
				if err != nil {
					panic(err)
				}
				defer outFile.Close()

				// Write the combined integer values to the output file
				for key, value := range keyIntMap {
					_, err := outFile.WriteString(fmt.Sprintf("%s %d\n", key, value))
					if err != nil {
						panic(err)
					}
				}

				// Write the concatenated string values to the output file
				for key, value := range keyStringMap {
					_, err := outFile.WriteString(fmt.Sprintf("%s %s\n", key, value))
					if err != nil {
						panic(err)
					}
				}

			}
			outputFile.Close()
			Reduce_Coordinator()
		}
	}
}

/*
	func combineValues(kvs []KeyValue) map[string][]string {
		groupedValues := make(map[string][]string)

		// Iterate over each KeyValue pair in the slice.
		for _, kv := range kvs {
			// Check if the key already exists in the map.
			if _, exists := groupedValues[kv.Key]; !exists {
				// If the key doesn't exist, initialize the slice for this key.
				groupedValues[kv.Key] = []string{}
			}
			// Append the value to the slice for this key.
			groupedValues[kv.Key] = append(groupedValues[kv.Key], kv.Value)
		}

		return groupedValues
	}
*/
func Call_Coordinator() Args_Task_Assignment_to_Worker { //return the reply to be used

	args := Args_Worker_Can_Work{Work_Yes_or_No: 1} //tell the coordinator that this worker can work

	reply := Args_Task_Assignment_to_Worker{Exit: 0} //get file path from Coordinator
	yes := Args_Task_Assignment_to_Worker{Exit: 1}
	num := call("Coordinator.Reply_Handler", &args, &reply) //send the rpc request
	if num == false {
		return yes
	}
	return reply
}

func Reduce_Coordinator() {
	args := Args_Status{}
	reply := Args_Status_Reply{}
	call("Coordinator.Reduce_Success_Or_Failure_Handler", &args, &reply)
}

func Status_Coordinator(status bool, path string, num int) { //RPC for worker to tell the coordinator that the task is finished

	args := Args_Status{SuccessorFailure: status, Path: path, Map_task_num: num} //create struct for status with status for successorfailure and the path to be removed from the slice
	reply := Args_Status_Reply{}                                                 //initialize reply
	call("Coordinator.Success_Or_Failure_Handler", &args, &reply)                //call to the Handler

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
