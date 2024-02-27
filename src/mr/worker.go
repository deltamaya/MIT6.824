package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	id := CallGetId().id
	// Your worker implementation here.
	for {
		task := CallFetch()
		if task.taskType == 0 {
			kvs := MapTask(task, mapf)
			MapEmit(id, kvs)
		} else if task.taskType == 1 {
			ReduceTask(task, reducef)
		} else {
			panic("unknown task type")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapTask(task *WcReply, mapf func(string, string) []KeyValue) []KeyValue {
	f := task.filename
	content, err := os.ReadFile(f)
	if err != nil {
		panic("read file failed")
	}
	return mapf(f, string(content))
}

func ReduceTask(task *WcReply, reducef func(string, []string) string) string {

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func MapEmit(id int, kvs []KeyValue) {
	sort.Sort(ByKey(kvs))
	ofname := fmt.Sprintf("mr-out-%d", id)
	f, _ := os.Create(ofname)
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		fmt.Fprint(f, "%v %v\n", kvs[i].Key, values)
		i = j
	}
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

func CallFetch() *WcReply {
	args := WcArgs{}
	reply := WcReply{}
	ok := call("Coordinator.Fetch", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("done")
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return &reply
}

func CallGetId() *IdReply {
	args := IdArgs{}
	reply := IdReply{}
	ok := call("Coordinator.GetId", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("done")
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return &reply
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
