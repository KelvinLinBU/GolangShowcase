package mr

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"

	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	reduce_task_count      int        //nReduce tasks
	file_slice             []string   //original files of data
	tasks_to_complete      []string   //slice for tasks to complete
	phase                  string     //map or reduce phase
	mu                     sync.Mutex //mutex
	map_task_nums          []int
	reduce_tasks_slice     []string
	reduce_tasks_completed int //how many reduce tasks completed
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) initialize_maptasknum_slice() []int {
	result := []int{}
	for i := 0; i < len(c.tasks_to_complete); i++ {
		result = append(result, i)
	}
	return result
}

func (c *Coordinator) Reply_Handler(args *Args_Worker_Can_Work, reply *Args_Task_Assignment_to_Worker) error {
	// Check if the worker is ready to work
	if args.Work_Yes_or_No != 1 {
		return errors.New("worker cannot work")
	}

	c.mu.Lock()

	switch c.phase {
	case "done":
		os.Exit(1)
	case "map":
		if len(c.tasks_to_complete) == 0 {
			reply.Maporreduce = "wait" // No map tasks left, instruct the worker to wait
			return nil
		}
		// Assign a map task to the worker
		reply.Maporreduce = c.phase
		reply.Tasknumber = c.map_task_nums[0]

		reply.Nreduce = c.reduce_task_count
		reply.Reply_path = c.tasks_to_complete[0]

		// Update the slices to reflect the assigned task
		c.tasks_to_complete = c.tasks_to_complete[1:]

		c.map_task_nums = c.map_task_nums[1:]

		// Update phase to wait if there are no more tasks to complete
		if len(c.tasks_to_complete) == 0 {
			c.phase = "wait"
		}
		c.mu.Unlock()
	case "wait":
		// Directly tell the worker to wait, nothing more to do
		reply.Maporreduce = "wait"
		c.mu.Unlock()

	case "reduce":
		// If in reduce phase, this implies all map tasks are done. Specific logic for assigning reduce tasks would go here.
		// This example does not include detailed logic for assigning reduce tasks, as it would depend on your overall design
		//go through the directory, split into buckets

		reply.Maporreduce = c.phase //reduce phase

		reply.Reply_path = c.reduce_tasks_slice[0]  //reduce tasks slice
		reply.Tasknumber = c.reduce_tasks_completed //task number
		// Update the slices to reflect the assigned task
		c.mu.Unlock()
		//this should be all the information the worker needs

	default:
		// Handle unexpected phase
		c.mu.Unlock()
		return fmt.Errorf("unhandled phase: %s", c.phase)

	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) Reduce_Success_Or_Failure_Handler(args *Args_Status, reply *Args_Status_Reply) error {

	c.mu.Lock()

	c.reduce_tasks_completed += 1

	c.reduce_tasks_slice = c.reduce_tasks_slice[1:] //

	if c.reduce_tasks_completed > c.reduce_task_count {

		/*fmt.Println("done")
		dir := "." // Directory to search, "." means the current directory
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}

		// Aggregate map
		aggregates := make(map[string]int)

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-out") {
				processFile(dir+"/"+file.Name(), aggregates)
			}
		}

		// Create or truncate the output file
		outputFile, err := os.Create("mr-out-*")
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outputFile.Close()

		// Write the aggregated results to the file
		for key, sum := range aggregates {
			_, err := outputFile.WriteString(fmt.Sprintf("%s %d\n", key, sum))
			if err != nil {
				log.Fatalf("Failed to write to output file: %v", err)
			}
		}
		*/

		c.phase = "done"
		remove()
		//os.Exit(1)
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Success_Or_Failure_Handler(args *Args_Status, reply *Args_Status_Reply) error {
	c.mu.Lock()

	filepathname := args.Path
	map_num := args.Map_task_num
	if args.SuccessorFailure { //if successful RPC
		//no need to append back

		if len(c.tasks_to_complete) == 0 {

			c.phase = "reduce"   //set phase to reduce
			dirPath := "."       // Directory containing MapReduce output files
			targetBaseDir := "." // Base directory for sorted output

			// Compile a regex to match files named mr-X-Y, capturing X and Y
			filePattern := regexp.MustCompile(`^mr-(\d+)-(\d+).json`)

			// Read the directory
			entries, err := os.ReadDir(dirPath)
			if err != nil {
				log.Fatalf("Error reading directory: %v", err)
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue // Skip directories
				}

				filename := entry.Name()
				matches := filePattern.FindStringSubmatch(filename)
				if matches == nil {
					continue // Skip files that do not match the pattern
				}

				reduceTaskNum := matches[2] // Capture group 2 is the reduce task number (Y)

				targetDir := filepath.Join(targetBaseDir, fmt.Sprintf("reduce-task-%s", reduceTaskNum))
				c.reduce_tasks_slice = append(c.reduce_tasks_slice, fmt.Sprintf("reduce-task-%s", reduceTaskNum)) //append to reduce tasks
				// Create the target directory if it doesn't exist
				if err := os.MkdirAll(targetDir, 0755); err != nil {
					log.Fatalf("Error creating directory %s: %v", targetDir, err)
				}

				// Copy the file to the target directory
				srcPath := filepath.Join(dirPath, filename)
				destPath := filepath.Join(targetDir, filename)
				if err := copyFile(srcPath, destPath); err != nil {
					log.Fatalf("Error copying file from %s to %s: %v", srcPath, destPath, err)
				} else {

				}
			}

			c.phase = "reduce"

			//set tasks to reduce
		}
		c.mu.Unlock()
		return nil
	} else { //if not successful RPC
		//append back the file path

		c.tasks_to_complete = append(c.tasks_to_complete, filepathname) //add task back to queue
		c.map_task_nums = append(c.map_task_nums, map_num)              //add corresponding number back to queue
		c.phase = "map"                                                 //change phase back to mapping phase
		c.mu.Unlock()
		return nil
	}
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

	dirPath, err := os.Getwd() // Example directory path
	if err != nil {
		fmt.Println(err)
	}
	c.mu.Lock()
	nReduce := c.reduce_task_count // Example number of reduce tasks
	c.mu.Unlock()
	if checkForAllReduceOutputs(dirPath, nReduce) {
		c.mu.Lock()
		c.phase = "done"
		c.mu.Unlock()
		ret = true
	} else {
		fmt.Println("Not done yet")
	}

	return ret
}

// checkForAllReduceOutputs checks if there are files named "mr-out-X" for each X up to nReduce.
func checkForAllReduceOutputs(dirPath string, nReduce int) bool {
	expectedFiles := make(map[int]bool)

	// Initialize map to track which reduce files we expect to find
	for i := 1; i <= nReduce; i++ {
		expectedFiles[i] = false
	}

	// Function to be called for each file and directory in dirPath
	checkFile := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // Propagate any error and stop walking the directory
		}
		if !info.IsDir() { // If this is a file
			var num int
			_, err := fmt.Sscanf(filepath.Base(path), "mr-out-%d", &num)
			if err == nil {
				// If the filename matches the expected format and the number is within our range
				if _, exists := expectedFiles[num]; exists {
					expectedFiles[num] = true
				}
			}
		}
		return nil
	}

	// Walk through the directory and check each file
	err := filepath.Walk(dirPath, checkFile)
	if err != nil {
		fmt.Printf("Error walking through directory: %v\n", err)
		return false
	}

	// Check if all expected files were found
	for _, found := range expectedFiles {
		if !found {
			return false // If any file was not found, return false
		}
	}

	return true // All expected files were found
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator { //takes as input a slice of strings, int for number of reduce tasks
	c := Coordinator{}
	fmt.Println("MakeCoordinator")
	c.file_slice = files          //sets slice of files in Coordinator struct
	c.reduce_task_count = nReduce //sets reduce_task_count in Coordinator struct
	c.tasks_to_complete = files
	c.reduce_tasks_slice = []string{}
	c.phase = "map"
	c.map_task_nums = c.initialize_maptasknum_slice()
	c.reduce_tasks_completed = 1

	c.server()

	return &c
}

func remove() {
	dir := "." // Specify the directory you want to scan

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		fileName := info.Name()

		// Check for directories specifically named "reduce" or files that contain "reduce" and end with ".json"
		if info.IsDir() {
			if strings.Contains(fileName, "reduce") {
				// Attempt to remove the directory

				removeErr := os.RemoveAll(path)
				if removeErr != nil {
					fmt.Printf("Error removing directory %s: %v\n", path, removeErr)
					return filepath.SkipDir // Skip this directory since it cannot be removed
				}
				return filepath.SkipDir // Skip to next directory since this one has been removed
			}
		} else if strings.Contains(fileName, "reduce") || strings.HasSuffix(fileName, ".json") || strings.Contains(fileName, "temp") {
			// This is a file that meets the criteria

			removeErr := os.Remove(path)
			if removeErr != nil {
				fmt.Printf("Error removing file %s: %v\n", path, removeErr)
			}
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error walking through directory %s: %v\n", dir, err)
	}
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

