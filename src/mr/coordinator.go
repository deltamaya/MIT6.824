package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Map struct {
	state      int    // 0 for idle, 1 for in-progress, 2 for completed
	workerId   int    // worker's id if the state is not idle
	location   string // emitted file location
	size       int    // emitted file size
	inputIndex int
	beginTime  time.Time
}
type Reduce struct {
	state     int
	workerId  int
	beginTime time.Time
}
type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	maps        []Map
	reduces     []Reduce
	workerCount int
	idLk        sync.Mutex
	taskLk      sync.RWMutex
	mapDone     bool
	mapLk       sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetId(args *HelloArgs, reply *HelloReply) error {
	c.idLk.Lock()
	c.workerCount++
	id := c.workerCount
	c.idLk.Unlock()
	reply.Id = id
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) Fetch(args *FetchArgs, reply *FetchReply) error {

	taskIndex := -1
	c.mapLk.RLock()
	defer c.mapLk.RUnlock()
	if !c.mapDone {
		// there is still map task needs to do
		c.taskLk.Lock()
		defer c.taskLk.Unlock()
		for i, m := range c.maps {
			if m.state == 0 || (m.state == 1 && time.Now().Sub(m.beginTime) >= time.Second*10) {
				taskIndex = i
				break
			}
		}
		// there is nothing to do
		if taskIndex == -1 {
			//use -1 to notice worker to wait
			reply.TaskType = -1
			return nil
		}
		task := &c.maps[taskIndex]
		task.beginTime = time.Now()
		task.state = 1
		task.workerId = args.WorkerId

		reply.TaskType = 0
		reply.TaskIndex = taskIndex
		reply.Filename = c.files[task.inputIndex]
	} else {
		// all map tasks are done
		c.taskLk.Lock()
		defer c.taskLk.Unlock()
		for i, r := range c.reduces {
			if r.state == 0 || (r.state == 1 && time.Now().Sub(r.beginTime) >= time.Second*10) {
				taskIndex = i
				break
			}
		}
		if taskIndex == -1 {
			//use -1 to notice worker to wait
			reply.TaskType = -1
			return nil
		}
		task := &c.reduces[taskIndex]
		task.beginTime = time.Now()
		task.state = 1
		task.workerId = args.WorkerId

		reply.TaskType = 1
		reply.TaskIndex = taskIndex
		reply.MapCount = len(c.maps)
	}
	return nil
}

func (c *Coordinator) ReportMapCompletion(args *MapReportArgs, reply *MapReportReply) error {
	c.taskLk.Lock()
	task := &c.maps[args.TaskIndex]
	task.state = 2
	task.location = args.Location
	task.size = args.Size
	c.taskLk.Unlock()
	for _, m := range c.maps {
		if m.state != 2 {
			return nil
		}
	}
	c.mapLk.Lock()
	c.mapDone = true
	c.mapLk.Unlock()
	return nil
}

func (c *Coordinator) ReportReduceCompletion(args *ReduceReportArgs, reply *ReduceReportReply) error {
	c.taskLk.Lock()
	task := &c.reduces[args.TaskIndex]
	task.state = 2
	c.taskLk.Unlock()
	return nil
}

func (c *Coordinator) SendEmittedFile(args *SendEmittedFileArgs, reply *SendEmittedFileReply) error {
	filename := args.FileName
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("can not read file")
	}
	reply.Content = string(content)

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
	ret := true

	// Your code here.
	for _, r := range c.reduces {
		if r.state != 2 {
			ret = false
			break
		}
	}
	if ret {
		log.Println("all done!")
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, p := range files {
		inputs, err := filepath.Glob(p)
		if err != nil {
			panic("syntax error")
		}
		c.files = append(c.files, inputs...)
	}
	c.mapDone = false
	c.nReduce = nReduce
	c.workerCount = 0
	// for i := 0; i < nReduce; i++ {
	// 	c.maps = append(c.maps, Map{
	// 		state:    0,
	// 		workerId: -1,
	// 	})
	// }
	for i, _ := range c.files {
		c.maps = append(c.maps, Map{
			state:      0,
			workerId:   -1,
			inputIndex: i,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduces = append(c.reduces, Reduce{
			state:    0,
			workerId: -1,
		})
	}

	c.server()
	return &c
}
