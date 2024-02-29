package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data       map[string]string
	requestIds map[int64]string // request id to return value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dup := kv.checkDuplication(args.Id)
	if dup {
		reply.Value = kv.requestIds[args.Id]
		return
	}
	v, ok := kv.data[args.Key]
	if ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
	kv.requestIds[args.Id] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dup := kv.checkDuplication(args.Id)
	if dup {
		return
	}
	kv.data[args.Key] = args.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	dup := kv.checkDuplication(args.Id)
	if dup {
		reply.Value = kv.requestIds[args.Id]
		return
	}
	v, ok := kv.data[args.Key]
	if ok {
		reply.Value = v
		v += args.Value
		kv.data[args.Key] = v
	} else {
		kv.data[args.Key] = args.Value
		reply.Value = ""
	}
	kv.requestIds[args.Id] = reply.Value

}

func (kv *KVServer) checkDuplication(id int64) bool {
	_, dup := kv.requestIds[id]
	return dup
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.data = make(map[string]string)
	kv.requestIds = make(map[int64]string)
	return kv
}
