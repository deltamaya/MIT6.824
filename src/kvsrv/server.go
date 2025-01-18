package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ResponseCache struct {
	requestID int
	value     string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]string
	// map clerk id to request id to reply
	reponses map[int]ResponseCache
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.data[args.Key]
	if !ok {
		value = ""
	}
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res, ok := kv.reponses[args.ClerkID]
	if ok && res.requestID == args.RequestID {
		reply.Value = args.Value
		return
	}
	kv.data[args.Key] = args.Value
	reply.Value = args.Value
	kv.reponses[args.ClerkID] = ResponseCache{requestID: args.RequestID, value: ""}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res, ok := kv.reponses[args.ClerkID]
	if ok && res.requestID == args.RequestID {
		reply.Value = res.value
		return
	}
	oldValue, ok := kv.data[args.Key]
	if !ok {
		oldValue = ""
	}
	kv.data[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	kv.reponses[args.ClerkID] = ResponseCache{requestID: args.RequestID, value: reply.Value}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.reponses = make(map[int]ResponseCache)
	return kv
}
