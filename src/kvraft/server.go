package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	RequestTimeout = 300 * time.Millisecond
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name      string
	Key       string
	Value     string
	ClerkID   int
	RequestID int
}

type ResponseCache struct {
	ClerkID   int
	RequestID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data           map[string]string
	responseCache  map[int]ResponseCache
	resultNotifyCh map[int]chan Result
}

type Result struct {
	clerkID   int
	requestID int
	value     string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Name:      "Get",
		Key:       args.Key,
		Value:     "",
		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		DPrintf("Peer %03d is not leader, rejecting request.\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	err, res := kv.waitCommand(op.ClerkID)
	reply.Err = err
	reply.Value = res.value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Name:      "Put",
		Key:       args.Key,
		Value:     args.Value,
		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		DPrintf("Peer %03d is not leader, rejecting request.\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	err, _ := kv.waitCommand(op.ClerkID)
	reply.Err = err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		Name:      "Append",
		Key:       args.Key,
		Value:     args.Value,
		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		DPrintf("Peer %03d is not leader, rejecting request.\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	err, _ := kv.waitCommand(op.ClerkID)
	reply.Err = err
}

func (kv *KVServer) waitCommand(clerkID int) (Err, Result) {
	timeout := time.NewTimer(RequestTimeout)
	defer timeout.Stop()
	resultCh := make(chan Result, 1)

	kv.mu.Lock()
	kv.resultNotifyCh[clerkID] = resultCh
	kv.mu.Unlock()

	select {
	case <-timeout.C:
		DPrintf("Server %03d timeout\n", kv.me)
		kv.removeResultCh(clerkID)
		return ErrTimeout, Result{}
	case res := <-resultCh:
		kv.removeResultCh(clerkID)
		return OK, res
	}
}

func (kv *KVServer) removeResultCh(clerkID int) {
	kv.mu.Lock()
	delete(kv.resultNotifyCh, clerkID)
	kv.mu.Unlock()
}

func (kv *KVServer) executeCommand(op Op) Result {
	res := Result{}
	res.clerkID = op.ClerkID
	res.requestID = op.RequestID
	if op.Name == "Get" {
		res.value = kv.data[op.Key]
	} else if op.Name == "Put" {
		kv.data[op.Key] = op.Value
		res.value = ""
	} else {
		oldValue, ok := kv.data[op.Key]
		if !ok {
			oldValue = ""
		}
		kv.data[op.Key] = oldValue + op.Value
		res.value = ""
	}
	return res
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.responseCache = make(map[int]ResponseCache)
	kv.resultNotifyCh = make(map[int]chan Result)

	// You may need initialization code here.
	go kv.handleApplyCh()
	return kv
}
func (kv *KVServer) handleApplyCh() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				idx := msg.CommandIndex
				DPrintf("Peer %03d got command %d, %v\n", kv.me, idx, msg.Command)
				op := msg.Command.(Op)
				kv.mu.Lock()
				res := kv.executeCommand(op)
				ch, ok := kv.resultNotifyCh[op.ClerkID]
				if ok {
					ch <- res
				}
				kv.mu.Unlock()
			}
		}
	}
}
