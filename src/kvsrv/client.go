package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const (
	MaxRetries = 3
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seq      int64
}

func (c *Clerk) CallAtLeastOnce(svcMeth string, args interface{}, reply interface{}) {
	for {
		ok := c.server.Call(svcMeth, args, reply)
		if ok {
			break
		}
		time.Sleep(time.Second * 1)
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		Seqence: Seq{RequestId: nrand(),
			ClientId:      ck.clientId,
			SeqenceNumber: ck.seq},
	}
	ck.seq++
	reply := GetReply{}
	ck.CallAtLeastOnce("KVServer.Get", &args, &reply)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Seqence: Seq{RequestId: nrand(), ClientId: ck.clientId, SeqenceNumber: ck.seq}}
	ck.seq++
	reply := PutAppendReply{}
	var ret string
	ck.CallAtLeastOnce("KVServer."+op, &args, &reply)
	ret = reply.Value
	return ret
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
