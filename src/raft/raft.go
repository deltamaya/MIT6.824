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

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

const (
	RPCTimeout        = 20 * time.Millisecond
	HeartbeatInterval = 50 * time.Millisecond
	ElectionTimeout   = 300 * time.Millisecond
	ApplyInterval     = 100 * time.Millisecond
	TickInterval      = 5 * time.Millisecond
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

	currentTerm      int
	votedFor         int
	electionTimer    *time.Timer
	syncEntriesTimer *time.Timer
	identity         int
	log              []LogEntry
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	applyCh          chan ApplyMsg
	notifyApplyCh    chan struct{}
	stopCh           chan struct{}
	appendEntryRetry int

	// represent the absolute index of the begining of the peer's log entries
	lastSnapshotIndex int
	lastSnapshotTerm  int
	currentSnapshot   []byte
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
	state := rf.getStateData()
	rf.persister.Save(state, rf.currentSnapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Unable to decode\n")
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastSnapshotIndex = lastSnapshotIndex
	rf.lastSnapshotTerm = lastSnapshotTerm
	rf.log = log

	rf.currentSnapshot = rf.getSnapshotData()

	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex
}

func (rf *Raft) getStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.log)
	state := w.Bytes()
	return state
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

	index = rf.logLengthAbs()
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.resetSyncTimeTrigger()
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
	rf.stopCh <- struct{}{}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	go func() {
		for {
			select {
			case <-rf.notifyApplyCh:
				rf.applyCommitedLogs()
			case <-rf.stopCh:
				return
			}
		}
	}()
	for !rf.killed() {
		rf.mu.Lock()
		// Your code here (3A)
		select {
		case <-rf.syncEntriesTimer.C:
			rf.syncEntries()
		case <-rf.electionTimer.C:
			rf.requestElection()
		default:
		}
		rf.mu.Unlock()
		time.Sleep(TickInterval)
	}
}

func (rf *Raft) applyCommitedLogs() {
	rf.mu.Lock()
	DPrintf("Applying log in Peer %03d, snapshot: %d, lastApplied: %d, commitIndex: %d with log %v\n", rf.me, rf.lastSnapshotIndex, rf.lastApplied, rf.commitIndex, rf.log)
	msgs := make([]ApplyMsg, 0, 20)
	if !(rf.lastApplied < rf.lastSnapshotIndex || rf.commitIndex <= rf.lastApplied) {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.logIndexAbs(i)].Command,
				CommandIndex: i,
			}
			msgs = append(msgs, msg)
		}
	}
	rf.mu.Unlock()
	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.mu.Lock()
		if msg.CommandIndex > rf.lastApplied {
			rf.lastApplied = msg.CommandIndex
		}

		rf.mu.Unlock()
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

func (rf *Raft) leaderToFollower() {
	DPrintf("Term %03d: Leader %03d became Follower\n", rf.currentTerm, rf.me)
	rf.identity = FOLLOWER
	rf.syncEntriesTimer.Stop()
	rf.electionTimer.Reset(randomElectionTimeout())
}
func (rf *Raft) candidateToLeader() {
	DPrintf("Term %03d: Candidate %03d became Leader\n", rf.currentTerm, rf.me)
	rf.identity = LEADER
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.appendEntryRetry = 0

	rf.resetSyncTimeTrigger()

	count := len(rf.peers)
	rf.nextIndex = make([]int, count)
	lastLogIndex := rf.logLengthAbs() - 1
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
	rf.syncEntriesTimer = time.NewTimer(HeartbeatInterval)
	rf.syncEntriesTimer.Stop()
	rf.applyCh = applyCh
	rf.stopCh = make(chan struct{}, 1)
	rf.log = []LogEntry{
		{},
	}
	rf.notifyApplyCh = make(chan struct{}, 10)
	rf.currentSnapshot = nil
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
