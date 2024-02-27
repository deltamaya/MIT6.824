package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

var NReduce, Id int

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
	reply := CallGetId()
	Id = reply.Id
	NReduce = reply.NReduce
	// fmt.Printf("worker id: %d\n", Id)
	// Your worker implementation here.
	for {
		task := CallFetch()
		if task.TaskType == 0 {
			kvs := MapTask(task, mapf)
			MapEmit(task.TaskIndex, kvs)
			CallReportMapCompletion(task.TaskIndex)
		} else if task.TaskType == 1 {
			ReduceTask(task, reducef)
			CallReportReduceCompletion(task.TaskIndex)
		} else if task.TaskType == 2 {

		} else {
			// fmt.Println("going to sleep")
			time.Sleep(10 * time.Second)
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapTask(task *FetchReply, mapf func(string, string) []KeyValue) []KeyValue {
	f := task.Filename
	content, err := os.ReadFile(f)
	if err != nil {
		panic("read file failed")
	}
	return mapf(f, string(content))
}

func ReduceTask(task *FetchReply, reducef func(string, []string) string) {
	kvs := []KeyValue{}
	for i := 0; i < task.MapCount; i++ {
		ifname := fmt.Sprintf("mr-%d-%d.txt", i, task.TaskIndex)
		// log.Printf("reduce file name: %s\n", ifname)
		content := CallSendEmittedFile(ifname)
		records := strings.Split(content, "\n")
		var k, v string
		for _, record := range records {
			_, err := fmt.Sscanf(record, "%v %v", &k, &v)
			if err != nil {
				break
			}
			kvs = append(kvs, KeyValue{
				Key:   k,
				Value: v,
			})
		}
	}
	sort.Sort(ByKey(kvs))
	ofname := fmt.Sprintf("mr-out-%d.txt", task.TaskIndex)
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
		result := reducef(kvs[i].Key, values)
		f, err := os.OpenFile(ofname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.Fatal("can not open reduce output file")
		}
		fmt.Fprintf(f, "%v %v\n", kvs[i].Key, result)
		f.Close()
		i = j
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func MapEmit(id int, kvs []KeyValue) {
	// for _,kv:=range(kvs){
	// 	ofname:=fmt.Sprintf("mr-intermediate-%d",id)
	// 	f,err:=os.OpenFile(ofname,os.O_CREATE|os.O_APPEND,0777)
	// 	if err!=nil{
	// 		panic("cant open file")
	// 	}
	// 	fmt.Fprintf(f,"%v %v\n",kv.Key,kv.Value)
	// }

	for _, kv := range kvs {
		index := ihash(kv.Key) % NReduce
		ofname := fmt.Sprintf("mr-%d-%d.txt", id, index)
		f, err := os.OpenFile(ofname, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
		if err != nil {
			panic("cant open file")
		}
		// fmt.Fprintf(f,"%v %v\n",kv.Key,kv.Value)
		fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
		f.Close()
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

func CallFetch() *FetchReply {
	args := FetchArgs{}
	reply := FetchReply{}
	ok := call("Coordinator.Fetch", &args, &reply)
	if ok {
		//fmt.Println("fetch done")
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return &reply
}

func CallGetId() *HelloReply {
	args := HelloArgs{}
	reply := HelloReply{}
	ok := call("Coordinator.GetId", &args, &reply)
	if ok {
		// fmt.Println("get id done")

	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return &reply
}

func CallReportMapCompletion(taskIndex int) {
	args := MapReportArgs{
		TaskIndex: taskIndex,
	}
	reply := MapReportReply{}
	ok := call("Coordinator.ReportMapCompletion", &args, &reply)
	if ok {
		// fmt.Println("report map completion done")

	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
}

func CallReportReduceCompletion(taskIndex int) {
	args := ReduceReportArgs{
		TaskIndex: taskIndex,
	}
	reply := ReduceReportReply{}
	ok := call("Coordinator.ReportReduceCompletion", &args, &reply)
	if ok {
		// fmt.Println("report reduce completion done")

	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
}

func CallSendEmittedFile(filename string) string {
	args := SendEmittedFileArgs{
		FileName: filename,
	}
	reply := SendEmittedFileReply{}
	ok := call("Coordinator.SendEmittedFile", &args, &reply)
	if ok {
		// fmt.Println("fetch emitted file done")
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return reply.Content
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
