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

const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
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

	currentTerm     int
	votedFor        int
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	identity        int
	Entries         []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.identity == LEADER
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
	CandidateID int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// Reply false if term < currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		log.Printf("Term %03d: Peer %03d against Peer %03d, low candidate term %03d\n", rf.currentTerm, rf.me, args.CandidateID, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		if rf.identity == LEADER {
			rf.heartbeatTicker = nil
			rf.identity = FOLLOWER
		}
	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.electionTimer = time.NewTimer(randomElectionTimeout())
		log.Printf("Term %03d: Peer %03d voted for Peer %03d\n", rf.currentTerm, rf.me, rf.votedFor)
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	log.Printf("Term %03d: Peer %03d against Peer %03d, already voted for %03d\n", rf.currentTerm, rf.me, args.CandidateID, rf.votedFor)

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	for !rf.killed() {
		rf.mu.Lock()
		// Your code here (3A)
		// Check if a leader election should be started.

		switch rf.identity {
		case LEADER:
			select {
			case <-rf.heartbeatTicker.C:
				rf.sendHeartbeat()
				rf.mu.Unlock()
			default:
				// do nothing
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)

			}
		case CANDIDATE:
			select {
			case <-rf.electionTimer.C:
				rf.requestElection()
				rf.mu.Unlock()
			default:
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		case FOLLOWER:
			select {
			case <-rf.electionTimer.C:
				rf.identity = CANDIDATE
				rf.requestElection()
				rf.mu.Unlock()
			default:
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}

	}
}

func (rf *Raft) requestElection() {

	// vote for myself by default
	voteCount := 1
	rf.votedFor = rf.me

	rf.currentTerm++
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	log.Printf("Term %03d: Candidate %03d requesting election.\n", rf.currentTerm, rf.me)

	total := len(rf.peers)
	replies := make([]RequestVoteReply, total)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		args := RequestVoteArgs{CandidateID: rf.me, Term: rf.currentTerm}
		go func(idx int) {
			rf.sendRequestVote(idx, &args, &replies[idx])
		}(idx)
	}

	time.Sleep(100 * time.Millisecond)

	for _, reply := range replies {
		if reply.VoteGranted {
			voteCount++
		}
	}
	log.Printf("Term %03d: Candidate %03d got %03d votes\n", rf.currentTerm, rf.me, voteCount)
	// vote is greater than half, become leader
	if rf.isMajority(voteCount) {
		rf.candidateToLeader()
		rf.sendHeartbeat()
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppednEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendHeartbeat() {

	log.Printf("Term %03d: Peer %03d sending heartbeats\n", rf.currentTerm, rf.me)
	args := AppendEntryArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
		Entries:  nil,
	}
	total := len(rf.peers)
	replies := make([]AppednEntryReply, total)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			rf.sendAppendEntry(idx, &args, &replies[idx])
		}(idx)
	}

	time.Sleep(30 * time.Millisecond)
	ack := 1
	for _, reply := range replies {
		if reply.Success {
			ack++
		}
	}

	// leader has disconnected to most peers
	if !rf.isMajority(ack) {
		log.Printf("Term %03d: Leader %03d didn't receive enough heartbeats\n", rf.currentTerm, rf.me)
		rf.leaderToFollower()
	}
}

func (rf *Raft) leaderToFollower() {
	log.Printf("Term %03d: Leader %03d became Follower\n", rf.currentTerm, rf.me)
	rf.identity = FOLLOWER
	rf.heartbeatTicker = nil
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
}
func (rf *Raft) candidateToLeader() {
	log.Printf("Term %03d: Candidate %03d became Leader\n", rf.currentTerm, rf.me)
	rf.identity = LEADER
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTicker = time.NewTicker(50 * time.Millisecond)
}

func (rf *Raft) isMajority(n int) bool {
	return n >= ((len(rf.peers) + 1) / 2)
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppednEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppednEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Term %03d: Peer %03d received heartbeat\n", rf.currentTerm, rf.me)
	switch rf.identity {
	case FOLLOWER:
		if rf.currentTerm <= args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1

			reply.Success = true
			reply.Term = args.Term
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
	case LEADER:
		if rf.currentTerm < args.Term {
			log.Printf("Term %03d: Leader %03d received higher term append entry at %03d\n", rf.currentTerm, rf.me, args.Term)
			rf.currentTerm = args.Term
			rf.leaderToFollower()

			reply.Success = true
			reply.Term = args.Term
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
	case CANDIDATE:
		if rf.currentTerm <= args.Term {
			log.Printf("Term %03d: Candidate %03d become Follower\n", rf.currentTerm, rf.me)
			rf.currentTerm = args.Term
			rf.identity = FOLLOWER
			rf.votedFor = -1

			reply.Success = true
			reply.Term = args.Term
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
	}
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.identity = FOLLOWER
	rf.heartbeatTicker = nil
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func randomElectionTimeout() time.Duration {
	amount := (rand.Int63() % 150) + 150
	return time.Duration(amount) * time.Millisecond
}
