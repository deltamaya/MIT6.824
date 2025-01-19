package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
	id           int
	requestIndex int
	leaderID     int
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
		RequestID: ck.requestIndex,
	}
	reply := GetReply{}
outer:
	for {
		DPrintf("Calling Get to peer %03d\n", ck.leaderID)
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			break
		}
		for i, server := range ck.servers {
			DPrintf("Calling Get to peer %03d\n", i)
			ok := server.Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.leaderID = i
				break outer
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.requestIndex++
	return reply.Value
}

// shared by Put and Append.
//
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
		RequestID: ck.requestIndex,
	}
	reply := PutAppendReply{}
outer:
	for {
		DPrintf("Calling %s to peer %03d\n", op, ck.leaderID)

		ok := ck.servers[ck.leaderID].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			break
		}
		for i, server := range ck.servers {
			DPrintf("try Calling %s to peer %03d\n", op, i)
			ok := server.Call("KVServer."+op, &args, &reply)
			if ok && reply.Err == OK {
				ck.leaderID = i
				break outer
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.requestIndex++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
