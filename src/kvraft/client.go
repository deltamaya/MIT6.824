package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

const (
	RPCTimeout = 10 * time.Millisecond
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	id        int
	curLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = int(nrand())
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:       key,
		ClerkID:   ck.id,
		RequestID: int(nrand()),
	}
	value := ""
	for {
		reply := GetReply{}
		DPrintf("Calling Get to server %03d\n", ck.curLeader)
		ok := ck.servers[ck.curLeader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			value = reply.Value
			break
		}
		ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		time.Sleep(RPCTimeout)
	}
	return value
}

// shared by Put and Append.
// 7
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClerkID:   ck.id,
		RequestID: int(nrand()),
	}
	for {
		reply := PutAppendReply{}
		DPrintf("Calling %s to server %03d\n", op, ck.curLeader)
		ok := ck.servers[ck.curLeader].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			break
		}
		ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		time.Sleep(RPCTimeout)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
