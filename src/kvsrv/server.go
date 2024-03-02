package kvsrv

import (
	"fmt"
	"log"
	"os"
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
	dupEntries map[int64]DupEntry
}

type DupEntry struct {
	Seqence   Seq
	Reply     string
	InStorage bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dup := kv.checkDuplication(args.Seqence, &reply.Value)
	if dup {
		return
	}

	v, ok := kv.data[args.Key]
	if ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
	kv.modifyEntries(args.Seqence, &reply.Value)

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dup := kv.checkDuplication(args.Seqence, &reply.Value)
	if dup {
		return
	}
	kv.data[args.Key] = args.Value
	kv.modifyEntries(args.Seqence, &reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	dup := kv.checkDuplication(args.Seqence, &reply.Value)
	if dup {
		return
	}
	v, ok := kv.data[args.Key]
	if ok {
		reply.Value = v
		kv.data[args.Key] += args.Value
	} else {
		kv.data[args.Key] = args.Value
		reply.Value = ""
	}
	kv.modifyEntries(args.Seqence, &reply.Value)
}

func (kv *KVServer) checkDuplication(seq Seq, response *string) bool {

	cli, ok := kv.dupEntries[seq.ClientId]
	if !ok {
		//no such client
		return false
	} else {
		if cli.Seqence.SeqenceNumber >= seq.SeqenceNumber {
			//expired or duplicate request
			if cli.InStorage {
				fname := fmt.Sprintf("%d.txt", cli.Seqence.ClientId)
				content, err := os.ReadFile(fname)
				if err != nil {
					log.Fatalf("can not read file:%s, error: %s", fname, err)
				}
				*response = string(content)
			} else {
				*response = kv.dupEntries[seq.ClientId].Reply
			}
			return true
		} else {
			//new request
			return false
		}
	}

}

func (kv *KVServer) modifyEntries(reqSeq Seq, response *string) {
	if len(kv.dupEntries) >= 100 {
		for _, s := range kv.dupEntries {
			if s.InStorage {
				err := os.Remove(s.Reply)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
		kv.dupEntries = make(map[int64]DupEntry)
	}
	current := DupEntry{}
	if len(*response) > 256*1024 {
		current.InStorage = true
		fname := fmt.Sprintf("%d.txt", reqSeq.ClientId)
		current.Reply = fname
		f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.Fatalf("can not create file: %s, error: %s", fname, err)
		}
		fmt.Fprintf(f, "%s", *response)
		f.Close()
	} else {
		current.InStorage = false
		current.Reply = *response
	}

	current.Seqence = reqSeq
	kv.dupEntries[reqSeq.ClientId] = current
}

// func (kv *KVServer) modifyResponse(requestId int64, response *string) {
// 	kv.knownRequestId[requestId] = *response
// 	if len(kv.knownRequestId) > 50 {
// 		kv.knownRequestId = make(map[int64]string)
// 	}
// }

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.data = make(map[string]string)
	kv.dupEntries = make(map[int64]DupEntry)
	return kv
}
