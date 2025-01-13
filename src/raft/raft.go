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
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
	notifyApplyCh   chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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
	// Reply false if term <= currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Term %03d: Peer %03d against Peer %03d, low candidate term %03d\n", rf.currentTerm, rf.me, args.CandidateID, args.Term)
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		DPrintf("Term %03d: Peer %03d against Peer %03d, outdated log %03d, at term %03d\n", rf.currentTerm, rf.me, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.electionTimer = time.NewTimer(randomElectionTimeout())
		DPrintf("Term %03d: Peer %03d voted for Peer %03d\n", rf.currentTerm, rf.me, rf.votedFor)
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	DPrintf("Term %03d: Peer %03d against Peer %03d, already voted for %03d\n", rf.currentTerm, rf.me, args.CandidateID, rf.votedFor)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.identity == LEADER

	// this node is not leader, can not handle requests
	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.log)

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})

	go rf.syncEntries()
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
		if rf.identity == LEADER {
			select {
			case <-rf.heartbeatTicker.C:
				rf.sendHeartbeat()
			default:
			}
		}

		select {
		case <-rf.electionTimer.C:
			rf.requestElection()
		case <-rf.notifyApplyCh:
			rf.applyCommitedLogs()
		default:
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applyCommitedLogs() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
}

func (rf *Raft) requestElection() {

	// vote for myself by default
	voteCount := 1
	rf.votedFor = rf.me
	rf.identity = CANDIDATE

	rf.currentTerm++
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	DPrintf("Term %03d: Candidate %03d requesting election.\n", rf.currentTerm, rf.me)

	total := len(rf.peers)
	replies := make([]RequestVoteReply, total)

	mtx := sync.Mutex{}
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		lastLogIndex := len(rf.log) - 1
		args := RequestVoteArgs{
			CandidateID:  rf.me,
			Term:         rf.currentTerm,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.log[lastLogIndex].Term}
		go func(idx int) {
			ok := rf.sendRequestVote(idx, &args, &replies[idx])
			if ok && replies[idx].VoteGranted {
				mtx.Lock()
				defer mtx.Unlock()
				voteCount++
			}
		}(idx)
	}

	time.Sleep(100 * time.Millisecond)
	mtx.Lock()
	defer mtx.Unlock()
	DPrintf("Term %03d: Candidate %03d got %03d votes\n", rf.currentTerm, rf.me, voteCount)
	// vote is greater than half, become leader
	if rf.isMajority(voteCount) {
		rf.candidateToLeader()
		rf.syncEntries()
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

type AppendEntryReply struct {
	Term         int
	Success      bool
	NextLogTerm  int
	NextLogIndex int
}

func (rf *Raft) getAppendEntries(peerID int) (int, int, []LogEntry) {
	nextIndex := rf.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := rf.log[nextIndex:]
	return prevLogIndex, prevLogTerm, entries

}

func (rf *Raft) syncEntries() {
	DPrintf("Term %03d: Peer %03d syncing entires\n", rf.currentTerm, rf.me)

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.syncEntriesSingle(idx)
	}

	time.Sleep(30 * time.Millisecond)

}

func (rf *Raft) syncEntriesSingle(peerID int) {
	rf.mu.Lock()
	prevLogIndex, prevLogTerm, logs := rf.getAppendEntries(peerID)

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntryReply{}
	DPrintf("Term %03d Leader %03d ae logs: %v\n", rf.currentTerm, rf.me, args.Entries)
	rf.sendAppendEntry(peerID, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		if reply.NextLogIndex > rf.nextIndex[peerID] {
			rf.nextIndex[peerID] = reply.NextLogIndex
			rf.matchIndex[peerID] = reply.NextLogIndex - 1
		}
		hasCommit := false
		for i := rf.commitIndex + 1; i <= rf.matchIndex[peerID]; i++ {
			count := 1
			for j := 0; j < len(rf.peers); j++ {
				if i >= rf.matchIndex[j] {
					count++
				}
			}
			if rf.isMajority(count) {
				rf.commitIndex = i
				hasCommit = true
			}
		}
		if hasCommit {
			DPrintf("Term %03d Leader %03d update commit index -> %d\n", rf.currentTerm, rf.me, rf.commitIndex)
			rf.notifyApplyCh <- struct{}{}
		}
		return
	}
	if reply.NextLogIndex != 0 {
		rf.nextIndex[peerID] = reply.NextLogIndex
	}

}

func (rf *Raft) sendHeartbeat() {
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	DPrintf("Term %03d: Peer %03d sending heartbeats\n", rf.currentTerm, rf.me)
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	total := len(rf.peers)
	replies := make([]AppendEntryReply, total)
	reachable := 1
	mtx := sync.Mutex{}
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			ok := rf.sendAppendEntry(idx, &args, &replies[idx])
			if ok && replies[idx].Success {
				mtx.Lock()
				defer mtx.Unlock()
				reachable++
			}
		}(idx)
	}

	time.Sleep(30 * time.Millisecond)
	mtx.Lock()
	defer mtx.Unlock()

	// leader has disconnected to most peers
	if !rf.isMajority(reachable) {
		DPrintf("Term %03d: Leader %03d didn't receive enough heartbeats\n", rf.currentTerm, rf.me)
		rf.leaderToFollower()
		return
	}
}

func (rf *Raft) leaderToFollower() {
	DPrintf("Term %03d: Leader %03d became Follower\n", rf.currentTerm, rf.me)
	rf.identity = FOLLOWER
	rf.heartbeatTicker = nil
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
}
func (rf *Raft) candidateToLeader() {
	DPrintf("Term %03d: Candidate %03d became Leader\n", rf.currentTerm, rf.me)
	rf.identity = LEADER
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTicker = time.NewTicker(50 * time.Millisecond)
	count := len(rf.peers)
	rf.nextIndex = make([]int, count)
	lastLogIndex := len(rf.log) - 1
	for i := 0; i < count; i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, count)
	for i := 0; i < count; i++ {
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) isMajority(n int) bool {
	return n >= ((len(rf.peers) + 1) / 2)
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	timeout := time.NewTimer(30 * time.Millisecond)
	defer timeout.Stop()
	ret := make(chan struct{}, 1)
	go func() {
		for i := 0; i < 10; i++ {
			ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
			if ok {
				ret <- struct{}{}
				break
			}
		}
	}()
	select {
	case <-ret:
		return true
	case <-timeout.C:
		DPrintf("Term %03d Peer %03d Append Entry to %03d timeout\n", args.Term, rf.me, server)
		return false
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.identity {
	case FOLLOWER:
	case CANDIDATE:
		rf.identity = FOLLOWER
	case LEADER:
		rf.leaderToFollower()
	}

	// if this is an outdated call
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.electionTimer = time.NewTimer(randomElectionTimeout())

	rf.currentTerm = args.Term
	rf.votedFor = -1

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.notifyApplyCh <- struct{}{}
		DPrintf("Term %03d Follower %03d update commit to %03d\n", rf.currentTerm, rf.me, rf.commitIndex)
	}

	// return if this is a heartbeat call
	if args.Entries == nil {
		reply.Success = true
		return
	}

	// has log gap
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextLogIndex = lastLogIndex + 1
		DPrintf("Term %03d Peer %03d refuse ae log gap, next: %03d\n", rf.currentTerm, rf.me, reply.NextLogIndex)
		return
	}

	// term mismatch
	if lastLogIndex != 0 && lastLogTerm != args.PrevLogTerm {
		reply.Success = false
		index := args.PrevLogIndex
		for index > rf.commitIndex && rf.log[index].Term != args.PrevLogTerm {
			index--
		}
		reply.NextLogIndex = index + 1
		DPrintf("Term %03d Peer %03d refuse ae for term mismatch, next: %03d\n", rf.currentTerm, rf.me, reply.NextLogIndex)
		return
	}

	// matched, replicate logs
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	reply.NextLogIndex = len(rf.log)
	DPrintf("Term %03d Peer %03d accepted ae, next: %03d\n", rf.currentTerm, rf.me, reply.NextLogIndex)

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
	rf.applyCh = applyCh
	rf.log = []LogEntry{
		{},
	}
	rf.notifyApplyCh = make(chan struct{}, 10)
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
