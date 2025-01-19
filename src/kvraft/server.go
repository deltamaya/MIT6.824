package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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
	data          map[string]string
	responseCache map[int]ResponseCache
	resultCh      chan Result
}

type Result struct {
	ClerkID   int
	RequestID int
	Value     string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Name:      "Get",
		Key:       args.Key,
		Value:     "",
		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}
	idx, _, isleader := kv.rf.Start(op)
	if !isleader {
		DPrintf("Peer %03d is not leader, rejecting request.\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	go kv.handleGetReply(args, reply)

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Name:      "Put",
		Key:       args.Key,
		Value:     args.Value,
		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}
	idx, _, isleader := kv.rf.Start(op)
	if !isleader {
		DPrintf("Peer %03d is not leader, rejecting request.\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	for {
		msg := <-kv.applyCh
		DPrintf("Peer %03d got msg %v.\n", kv.me, msg)

		if msg.CommandValid &&
			idx == msg.CommandIndex &&
			msg.Command != nil {
			op = msg.Command.(Op)
			if op.ClerkID == args.ClerkID &&
				op.RequestID == args.RequestID {
				value := args.Value
				kv.data[args.Key] = value
				reply.Err = OK
				DPrintf("Peer %03d replied request.\n", kv.me)
				break
			}
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Name:      "Append",
		Key:       args.Key,
		Value:     args.Value,
		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}
	idx, _, isleader := kv.rf.Start(op)
	if !isleader {
		DPrintf("Peer %03d is not leader, rejecting request.\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	for {
		msg := <-kv.applyCh
		DPrintf("Peer %03d got msg %v.\n", kv.me, msg)

		if msg.CommandValid &&
			idx == msg.CommandIndex &&
			msg.Command != nil {
			op = msg.Command.(Op)
			if op.ClerkID == args.ClerkID &&
				op.RequestID == args.RequestID {
				oldValue, ok := kv.data[args.Key]
				if !ok {
					oldValue = ""
				}
				kv.data[args.Key] = oldValue + args.Value
				reply.Err = OK
				DPrintf("Peer %03d replied request.\n", kv.me)
				break
			}
		}
	}
}

func (kv *KVServer) executeCommand(op Op) string {
	if op.Name == "Get" {
		return kv.data[op.Key]
	} else if op.Name == "Put" {
		kv.data[op.Key] = op.Value
		return ""
	} else {
		oldValue, ok := kv.data[op.Key]
		if !ok {
			oldValue = ""
		}
		kv.data[op.Key] = oldValue + op.Value
		return ""
	}
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

	// You may need initialization code here.
	return kv
}

func (kv *KVServer) handleGetReply(targetIndex int,args *GetArgs, reply *GetReply) {
	for {
		msg := <-kv.applyCh
		DPrintf("Peer %03d got msg %v.\n", kv.me, msg)
		if msg.CommandValid {
			if msg.CommandIndex==
		} else if msg.SnapshotValid {

		}
		if msg.CommandValid &&
			idx == msg.CommandIndex &&
			msg.Command != nil {
			op = msg.Command.(Op)
			if op.ClerkID == args.ClerkID &&
				op.RequestID == args.RequestID {
				value := kv.data[args.Key]
				reply.Value = value
				reply.Err = OK
				DPrintf("Peer %03d replied request.\n", kv.me)
				break
			}
		}
	}
}
