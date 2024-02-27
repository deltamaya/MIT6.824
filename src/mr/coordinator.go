package mr

import (
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"sync"
	"time"
)

type Map struct {
	state    int    // 0 for idle, 1 for in-progress, 2 for completed
	workerId []int    // worker's id if the state is not idle
	location string // emitted file location
	size     int    //emitted file size
	beginTime time.Time
}
type Reduce struct {
	state    int
	workerId int
}
type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	nMap    int
	maps    []Map
	reduces []Reduce
	workerCount int
	lk sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Fetch(args *WcArgs, reply *WcReply) error {
	taskId:=-1
	c.lk.Lock()
	for i,m :=range(c.maps){
		if m.state==0||(m.state==1&&time.Now().Sub(m.beginTime)>=time.Second*10){
			taskId=i
			break
		}
	}

	c.maps[taskId].beginTime=time.Now()
	c.maps[taskId].state=1
	c.maps[taskId].workerId = append(c.maps[taskId].workerId, args.workerId)
	c.lk.Unlock()

	reply.taskType=0
	reply.filename=
	return nil
}

func (c *Coordinator) GetId(args *IdArgs, reply *IdReply) error {
	c.lk.Lock()
	c.workerCount++
	id:=c.workerCount
	c.lk.Unlock()
	reply.id=id
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
	//this is some comment added by me
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
	ret = len(c.done) == len(c.files)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	entries, err := os.ReadDir(".")
	regularFiles := []fs.DirEntry{}
	if err != nil {
		fmt.Println("read dir error")
	}
	for _, e := range entries {
		if !e.IsDir() {
			regularFiles = append(regularFiles, e)
		}
	}
	for _, e := range regularFiles {
		for _, p := range files {
			if m, _ := regexp.MatchString(p, e.Name()); m == true {
				c.files = append(c.files, e.Name())
				break
			}
		}
	}
	c.nReduce = nReduce
	c.nMap = nReduce
	c.workerCount=0
	index := []int{}
	for i, _ := range files {
		index = append(index, i)
	}
	

	c.server()
	return &c
}
