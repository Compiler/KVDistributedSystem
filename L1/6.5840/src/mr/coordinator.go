package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	inputFiles   []string
	reducedFiles []string
	outputFiles  []string
	numReduce    int
	lock         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Task(reply *WorkerTaskReply) error {
	fmt.Println("We got called from coordinator")
	c.lock.Lock()
	fileToParse := c.inputFiles[0]
	c.inputFiles = c.inputFiles[1:]
	c.lock.Unlock()

	reply.DoMap = true
	reply.File = fileToParse
	reply.WriteTo = "."
	return nil
}

unc (c *Coordinator) DoneMap(reply *WorkerTaskReply) error {
	fmt.Println("We got called from coordinator")
	c.lock.Lock()
	fileToParse := c.inputFiles[0]
	c.inputFiles = c.inputFiles[1:]
	c.lock.Unlock()

	reply.DoMap = true
	reply.File = fileToParse
	reply.WriteTo = "."
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = len(c.inputFiles) == 0 && len(c.reducedFiles) == c.numReduce

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, make([]string, 0), make([]string, 0), nReduce, sync.Mutex{}}

	// Your code here.

	c.server()
	return &c
}
