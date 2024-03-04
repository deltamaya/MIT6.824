package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	HeartbeatInterval = 100 // 100ms
	MaxRetries        = 3
	RetriesInterval   = 100 //100ms
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor         *labrpc.ClientEnd
	currentTerm      int
	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time
	// currentLeader    *labrpc.ClientEnd
	isLeader bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term    int
	IsVoted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type LogEntry struct {
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm >= args.Term {
		reply.IsVoted = false
		log.Printf("Term: %d, Client: %d Vote Against %d(EXPIRED)\n", rf.currentTerm, rf.me, args.CandidateId)
		return
	}
	if rf.votedFor == nil && time.Since(rf.lastHeartbeat) > 2*HeartbeatInterval*time.Millisecond {
		//missed at least 4 leader heartbeat
		reply.IsVoted = true
		reply.Term = rf.currentTerm + 1
		rf.votedFor = rf.peers[args.CandidateId]
		log.Printf("Term: %d, Client: %d Vote For %d\n", rf.currentTerm, rf.me, args.CandidateId)
		time.AfterFunc(500*time.Millisecond, func() {
			rf.mu.Lock()
			func(term int) {
				if term == rf.currentTerm {
					rf.votedFor = nil
				}
			}(rf.currentTerm)
			rf.mu.Unlock()
		})
	} else {
		reply.IsVoted = false
		var reason string
		if rf.votedFor == nil {
			reason = "HeartBeat"
		} else {
			reason = "VOTED"
		}
		log.Printf("Term: %d, Client: %d Vote Against %d(%s)\n", rf.currentTerm, rf.me, args.CandidateId, reason)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// if rf.isLeader {
	// 	rf.isLeader = false
	// 	log.Printf("Term: %d, Client: %d I'm now Leader now\n", rf.currentTerm, rf.me)
	// }
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.isLeader = false
	}
	reply.Success = true
	rf.lastHeartbeat = time.Now()
	rf.heartbeatTimeout = randomHeartbeatTimeout()
	// log.Printf("Term: %d, Client: %d Received Heartbeat from %d\n", rf.currentTerm, rf.me, args.LeaderId)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, lk *sync.Mutex, voteCount *int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.IsVoted {
		lk.Lock()
		*voteCount++
		lk.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok && reply.Success {
		rf.mu.Lock()
		rf.lastHeartbeat = time.Now()
		rf.heartbeatTimeout = randomHeartbeatTimeout()
		rf.mu.Unlock()
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here (3A)
		rf.mu.Lock()
		if rf.isLeader {
			if time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout {
				rf.isLeader = false
				log.Printf("Term: %d, Client: %d Leader quit\n", rf.currentTerm, rf.me)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				rf.CallAppendEntries()

			}
		} else {

			// Check if a leader election should be started.
			if time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout && rf.votedFor == nil {
				rf.mu.Unlock()
				voteCount := rf.CallRequestVote()
				// rf.votedFor = rf.peers[rf.me]

				// rf.mu.Lock()
				// log.Printf("Term: %d, Client: %d Request Vote Returned\n", rf.currentTerm, rf.me)
				// rf.mu.Unlock()

				if rf.isMajorityVote(voteCount) {
					//I'm leader now
					rf.mu.Lock()
					rf.isLeader = true
					rf.currentTerm++
					rf.votedFor = nil
					log.Printf("Term: %d, Client: %d New Leader, Vote Count: %d\n", rf.currentTerm, rf.me, voteCount)
					rf.mu.Unlock()

					rf.CallAppendEntries()
					// go rf.BeginHeartbeat()
				} else {
					rf.mu.Lock()
					rf.votedFor = nil
					log.Printf("Term: %d, Client: %d Not New Leader, Vote Count: %d\n", rf.currentTerm, rf.me, voteCount)
					rf.lastHeartbeat = time.Now()
					rf.heartbeatTimeout = randomHeartbeatTimeout()
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// func (rf *Raft) BeginHeartbeat() {
// 	rf.mu.Lock()
// 	log.Printf("Term: %d, Client: %d Heartbeat begin\n", rf.currentTerm, rf.me)
// 	rf.mu.Unlock()
// 	for {
// 		rf.mu.Lock()

// 		if rf.isLeader && time.Since(rf.lastHeartbeat) < rf.heartbeatTimeout {
// 			rf.mu.Unlock()
// 			rf.CallAppendEntries()

// 		} else {
// 			rf.isLeader = false
// 			rf.mu.Unlock()
// 			break
// 		}
// 		time.Sleep(HeartbeatInterval * time.Millisecond)
// 	}
// 	rf.mu.Lock()
// 	log.Printf("Term: %d, Client: %d Heartbeat END\n", rf.currentTerm, rf.me)
// 	rf.mu.Unlock()
// }

func (rf *Raft) CallRequestVote() int {
	rf.mu.Lock()
	rf.votedFor = rf.peers[rf.me]
	log.Printf("Term: %d, Client: %d Requesting vote\n", rf.currentTerm, rf.me)
	args := RequestVoteArgs{
		Term:        rf.currentTerm + 1,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()
	ret := make([]RequestVoteReply, len(rf.peers))
	var lk sync.Mutex
	voteCount := 1

	for i := range rf.peers {

		if i != rf.me {

			// rf.mu.Lock()

			// log.Printf("Term: %d, Client: %d Sending Vote Request to Client %d\n", rf.currentTerm, rf.me, i)
			// rf.mu.Unlock()
			go rf.sendRequestVote(i, &args, &ret[i], &lk, &voteCount)

		}

	}
	time.Sleep(30 * time.Millisecond)
	lk.Lock()
	defer lk.Unlock()
	return voteCount
}

func (rf *Raft) CallAppendEntries() []AppendEntriesReply {
	responses := make([]AppendEntriesReply, len(rf.peers))
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	var lk sync.Mutex
	for i := range rf.peers {
		if i != rf.me {

			go rf.sendAppendEntries(i, &args, &responses[i])
			// if ok {
			// 	responses = append(responses, &reply)
			// 	rf.mu.Lock()
			// 	rf.lastHeartbeat = time.Now()
			// 	rf.heartbeatTimeout = randomHeartbeatTimeout()
			// 	rf.mu.Unlock()
			// } else {
			// 	responses = append(responses, nil)
			// }
		}

	}
	time.Sleep(30 * time.Millisecond)
	lk.Lock()
	defer lk.Unlock()
	return responses
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	//Initialization code for 3A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = 0
	rf.heartbeatTimeout = randomHeartbeatTimeout()
	rf.isLeader = false
	rf.lastHeartbeat = time.Now()
	rf.votedFor = nil
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// generate a random duration
func randomHeartbeatTimeout() time.Duration {
	min := 5 * HeartbeatInterval
	max := 10 * HeartbeatInterval
	timeout := time.Millisecond * time.Duration(rand.Intn(max-min)+min)
	return timeout
}

func (rf *Raft) isMajorityVote(count int) bool {
	return count >= len(rf.peers)/2+1
}
