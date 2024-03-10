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
	RETRYINTERVAL     = 50
	HEARTBEATINTERVAL = 100
	CHECKINTERVAL     = 300 // 100ms
	MAXRETRIES        = 3
	// RetriesInterval   = 100 //100ms
	LEADER    ServerState = 1
	FOLLOWER  ServerState = 0
	CANDIDATE ServerState = 2
	RPCWAIT               = 20
)

type ServerState int

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
	votedFor      *labrpc.ClientEnd
	currentTerm   int
	electionTimer *time.Timer
	// electionTimeout time.Duration
	serverState   ServerState
	lastHeartbeat time.Time
	log           []LogEntry
	commitIndex   int
	lastLogIndex  int //last log index
	nextIndex     []int
	matchIndex    []int
	leaderId      int
	applyCh       chan ApplyMsg
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
	isleader = rf.serverState == LEADER
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
	// LastLogIndex int
	// LastLogTerm  int
	CommitIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term    int
	IsVoted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm >= args.Term {
		reply.Term = rf.currentTerm
		reply.IsVoted = false
		DPrintf("Term: %d, Client: %d | Voted against %d(EXPIRED)", rf.currentTerm, rf.me, args.CandidateId)
		return
	} else {
		DPrintf("Client: %d, vote request term is higher than me: %d, term: %d", rf.me, rf.currentTerm, args.Term)
		rf.ToTerm(args.Term)
		if rf.serverState != FOLLOWER {
			rf.serverState = FOLLOWER
			go rf.following()
		}
	}
	rf.resetElectionTimer()
	if rf.votedFor != nil {
		reply.IsVoted = false
		DPrintf("Term: %d, Client: %d | Voted against %d(VOTED)", rf.currentTerm, rf.me, args.CandidateId)
		return
	}
	if args.Term >= rf.currentTerm && args.CommitIndex >= rf.commitIndex {
		reply.IsVoted = true
		reply.Term = args.Term
		rf.votedFor = rf.peers[args.CandidateId]
		DPrintf("Term: %d, Client: %d | Voted for %d，cur commit: %d, candidate commit: %d", rf.currentTerm, rf.me, args.CandidateId, rf.commitIndex, args.CommitIndex)
		return
	} else {
		reply.IsVoted = false
		DPrintf("Term: %d, Client: %d | Voted against %d(STALE ENTRY)", rf.currentTerm, rf.me, args.CandidateId)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term: %d, Client: %d | Received heartbeat from %d", rf.currentTerm, rf.me, args.LeaderId)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.ToTerm(args.Term)
		if rf.serverState != FOLLOWER {
			rf.serverState = FOLLOWER
			go rf.following()
		}
	}

	rf.resetElectionTimer()
	// DPrintf("Client: %d reset timer", rf.me)
	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex > rf.lastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	if args.PrevLogIndex < rf.lastLogIndex && len(args.Entries) > 1 &&
		rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.log[args.PrevLogIndex+1] = args.Entries[0]
	}

	reply.Success = true
	rf.votedFor = nil
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	rf.lastLogIndex = len(rf.log) - 1
	var min int
	DPrintf("Client status: id:%d, lastlog: %d commit: %d", rf.me, rf.lastLogIndex, rf.commitIndex)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > args.PrevLogIndex+1 {
			min = args.PrevLogIndex + 1
		} else {
			min = args.LeaderCommit
		}
		for i := rf.commitIndex + 1; i <= min; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.commitIndex = min
	}

	DPrintf("Client: %d append success, last log index: %d, len(log): %d", rf.me, rf.lastLogIndex, len(rf.log))

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's //log. if this
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != LEADER {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	DPrintf("Term: %d, Client: %d | append log:%v, lastApplied: %d", rf.currentTerm, rf.me, newLog, rf.lastLogIndex)
	return newLog.Index, newLog.Term, true
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

// no lock inside
func (rf *Raft) NextTerm() {
	rf.currentTerm++
	rf.votedFor = nil
}

// no lock inside
func (rf *Raft) ToTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = nil
}
func (rf *Raft) CallRequestVote() int {

	rf.mu.Lock()
	rf.votedFor = rf.peers[rf.me]
	DPrintf("Term: %d, Client: %d | Requesting vote", rf.currentTerm, rf.me)
	rf.mu.Unlock()
	ret := make([]RequestVoteReply, len(rf.peers))
	var lk sync.Mutex
	voteCount := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
					// LastLogIndex: rf.lastLogIndex,
					// LastLogTerm:  rf.log[rf.lastLogIndex].Term,
					CommitIndex: rf.commitIndex,
				}
				rf.mu.Unlock()
				ok := rf.sendRequestVote(server, &args, &ret[server])
				if !ok {
					return
				}
				rf.resetElectionTimer()
				if ret[server].IsVoted {
					lk.Lock()
					voteCount++
					lk.Unlock()
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && ret[server].Term > rf.currentTerm {
					rf.ToTerm(ret[server].Term)
					if rf.serverState != FOLLOWER {
						rf.serverState = FOLLOWER
						go rf.following()
					}
				}

			}(i)
		}
	}

	time.Sleep(RPCWAIT * time.Millisecond)
	lk.Lock()
	defer lk.Unlock()
	return voteCount
}

func (rf *Raft) CallAppendEntries() []AppendEntriesReply {

	responses := make([]AppendEntriesReply, len(rf.peers))

	// DPrintf("Term: %d, Client: %d | Appending entries, last applied: %d, len(log): %d", rf.currentTerm, rf.me, rf.lastLogIndex, len(rf.log))

	var lk sync.Mutex
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				// re := 0

				for {
					rf.mu.Lock()
					if rf.nextIndex[server] == 0 || rf.serverState != LEADER {
						rf.mu.Unlock()
						// break
						return
					}
					DPrintf("leader %d next indices: %v", rf.me, rf.nextIndex)
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[server] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
						LeaderCommit: rf.commitIndex,
						Entries:      rf.log[rf.nextIndex[server]:],
					}
					DPrintf("creating append entry done: %v", args)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, &args, &responses[server])
					if !ok {
						// re++
						// continue
						return
					}
					rf.resetElectionTimer()
					rf.mu.Lock()
					if rf.serverState == LEADER && responses[server].Term > rf.currentTerm {
						rf.ToTerm(responses[server].Term)
						if rf.serverState != FOLLOWER {
							rf.serverState = FOLLOWER
							rf.mu.Unlock()
							go rf.following()
							// break
							return
						}
					}
					if !responses[server].Success {
						rf.nextIndex[server]--
						DPrintf("client: %d, decrease next index", server)
						rf.mu.Unlock()
					} else {
						rf.nextIndex[server] = rf.lastLogIndex + 1
						DPrintf("nextIndex[%d] = %d", server, rf.nextIndex[server])
						rf.matchIndex[server] = rf.lastLogIndex
						rf.mu.Unlock()
						// break
						return
					}
				}

			}(i)
		}
	}
	time.Sleep(RPCWAIT * time.Millisecond)
	rf.mu.Lock()
	DPrintf("leader status: %d %d.match: %v", rf.lastLogIndex, rf.commitIndex, rf.matchIndex)
	for j := rf.lastLogIndex; j > rf.commitIndex; j-- {
		count := 1
		for i, m := range rf.matchIndex {
			if i != rf.me && j <= m {
				count++
			}
		}
		if rf.isMajorityVote(count) && rf.log[j].Term == rf.currentTerm {
			for i := rf.commitIndex + 1; i <= j; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
			rf.commitIndex = j
			break
		}
	}
	rf.mu.Unlock()
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

	rf.ToTerm(0)
	rf.electionTimer = time.NewTimer(114514 * time.Second)
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.lastHeartbeat = time.Now()
	rf.serverState = FOLLOWER
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{})
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.lastLogIndex = 0
	rf.mu.Unlock()
	go rf.following()
	go rf.electionDetector()
	return rf
}

func (rf *Raft) following() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.serverState != FOLLOWER {
			DPrintf("Client %d Follower QUIT", rf.me)
			rf.mu.Unlock()
			return
		} else {
			DPrintf("Client %d is following now", rf.me)
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * HEARTBEATINTERVAL)
	}
}

func (rf *Raft) leading() {
	// var shutdown chan struct{}
	// go rf.heartbeat(shutdown)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.serverState != LEADER {
			rf.mu.Unlock()
			DPrintf("Client %d LEADER QUIT", rf.me)
			// shutdown <- struct{}{}
			return
		} else {
			DPrintf("Client %d is leader now", rf.me)
			rf.mu.Unlock()
			rf.CallAppendEntries()
		}
		time.Sleep(time.Millisecond * HEARTBEATINTERVAL)
	}
}

func (rf *Raft) heartbeat(shutdown <-chan struct{}) {
	for !rf.killed() {
		select {
		case <-shutdown:
			return
		default:
			rf.sendHeartbeat()
			time.Sleep(HEARTBEATINTERVAL * time.Millisecond)
		}
	}

}

func (rf *Raft) sendHeartbeat() {
	DPrintf("sending heartbeat")
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				responses := make([]AppendEntriesReply, len(rf.peers))
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server, &args, &responses[server])
				if !ok {
					return
				}
				rf.resetElectionTimer()
				rf.mu.Lock()
				if rf.serverState == LEADER && responses[server].Term > rf.currentTerm {
					rf.ToTerm(responses[server].Term)
					if rf.serverState != FOLLOWER {
						rf.serverState = FOLLOWER
						rf.mu.Unlock()
						go rf.following()
						return
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

func (rf *Raft) startElection() {

	vote := rf.CallRequestVote()
	if rf.isMajorityVote(vote) {
		rf.mu.Lock()
		DPrintf("Term: %d, Client: %d | New leader, vote count: %d", rf.currentTerm, rf.me, vote)
		rf.votedFor = nil
		if rf.serverState == CANDIDATE {
			rf.serverState = LEADER
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.lastLogIndex + 1
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			go rf.leading()
		}
		rf.mu.Unlock()
		rf.CallAppendEntries()
		// rf.sendHeartbeat()
	} else {
		rf.mu.Lock()
		DPrintf("Term: %d, Client: %d | NOT New leader, vote count: %d", rf.currentTerm, rf.me, vote)
		rf.mu.Unlock()
	}
	rf.resetElectionTimer()
}

func (rf *Raft) appendNewEntry(command interface{}) LogEntry {
	rf.lastLogIndex++
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command, Index: rf.lastLogIndex})
	return rf.log[rf.lastLogIndex]
}

func (rf *Raft) electionDetector() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.serverState != LEADER {
			rf.serverState = CANDIDATE
			rf.NextTerm()
			go rf.startElection()
		} else {
			if rf.serverState != FOLLOWER {
				rf.serverState = FOLLOWER
				go rf.following()
			}
		}
		rf.mu.Unlock()
	}
}

// generate a random duration
func (rf *Raft) resetElectionTimer() {
	min := 150
	max := 350
	timeout := time.Millisecond * time.Duration(rand.Intn(max-min)+min)
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(timeout)
}

func (rf *Raft) isMajorityVote(count int) bool {
	return count*2 > len(rf.peers)
}
