package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

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

func writePairsToFile(pairs []KeyValue, filename string) error {
	// Open the file for writing. Create it if it doesn't exist, and truncate it if it does.
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a new buffered writer.
	writer := bufio.NewWriter(file)

	// Write each KeyValue pair to the file.
	for _, pair := range pairs {
		line := fmt.Sprintf("%s %s\n", pair.Key, pair.Value)
		_, err := writer.WriteString(line)
		if err != nil {
			return err
		}
	}

	// Flush any buffered data to the underlying writer (the file).
	return writer.Flush()
}

func readPairsFromFile(filename string) ([]KeyValue, error) {
	// Open the file for reading.
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var pairs []KeyValue

	// Create a new buffered reader.
	scanner := bufio.NewScanner(file)

	// Read each line and split it into key-value pairs.
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line format: %s", line)
		}
		pairs = append(pairs, KeyValue{Key: parts[0], Value: parts[1]})
	}

	// Check for errors during scanning.
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return pairs, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// declare a reply structure.
	reply := WorkerTaskReply{}

	ok := call("Coordinator.Task", &reply)
	if ok {
		// reply.Y should be 100.
		if reply.DoMap {
			content, err := os.ReadFile(reply.File)
			if err != nil {
				fmt.Print("Fuck")
			} else {
				kvPairs := mapf(reply.File, string(content))
				uniqueFilename := string(ihash(time.Now().GoString()))
				writePairsToFile(kvPairs, uniqueFilename)
				call("Coordinator.DoneTask", {})
			}
		} else {
			content, err := os.ReadFile(reply.File)
		}
	} else {
		fmt.Println("Reply: ", reply)
		fmt.Printf("call failed!\n")
	}
	fmt.Print("We done")

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
