package raft

import (
	"math/rand"
	"sync"
	"time"
)

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
	timeout := time.NewTimer(RPCTimeout)
	defer timeout.Stop()
	ret := make(chan struct{}, 1)
	stopCh := make(chan struct{}, 1)
	go func() {
		for i := 0; i < 10; i++ {
			if rf.killed() {
				return
			}
			select {
			case <-stopCh:
				return
			default:
			}
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
		DPrintf("Term %03d Peer %03d Request Vote to %03d timeout\n", args.Term, rf.me, server)
		stopCh <- struct{}{}
		return false
	}
}

func (rf *Raft) requestElection() {
	if rf.killed() {
		return
	}
	// vote for myself by default
	voteCount := 1
	rf.votedFor = rf.me
	rf.identity = CANDIDATE
	rf.currentTerm++
	rf.persist()
	defer rf.persist()
	DPrintf("Term %03d: Candidate %03d requesting election.\n", rf.currentTerm, rf.me)

	rf.mu.Unlock()

	total := len(rf.peers)
	replies := make([]RequestVoteReply, total)

	mtx := sync.Mutex{}
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		rf.mu.Lock()

		lastLogIndex := rf.logLengthAbs() - 1
		args := RequestVoteArgs{
			CandidateID:  rf.me,
			Term:         rf.currentTerm,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.log[rf.logIndexAbs(lastLogIndex)].Term,
		}
		rf.mu.Unlock()

		go func(idx int) {
			ok := rf.sendRequestVote(idx, &args, &replies[idx])
			if ok && replies[idx].VoteGranted {
				mtx.Lock()
				defer mtx.Unlock()
				voteCount++
			}
		}(idx)
	}

	time.Sleep(RPCTimeout)
	rf.mu.Lock()
	if rf.killed() {
		return
	}
	mtx.Lock()
	defer mtx.Unlock()

	DPrintf("Term %03d: Candidate %03d got %03d votes\n", rf.currentTerm, rf.me, voteCount)
	// vote is greater than half, become leader
	if rf.isMajority(voteCount) {
		rf.candidateToLeader()
		rf.resetSyncTimeTrigger()
	}
	rf.electionTimer = time.NewTimer(randomElectionTimeout())

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// Reply false if term <= currentTerm
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Term %03d: Peer %03d against Peer %03d, low candidate term %03d\n", rf.currentTerm, rf.me, args.CandidateID, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// args.Term == rf.currentTerm
	lastLogIndex := rf.logLengthAbs() - 1
	lastLogTerm := rf.log[rf.logIndexAbs(lastLogIndex)].Term
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
		if rf.identity == LEADER {
			rf.syncEntriesTimer.Stop()
			rf.identity = FOLLOWER
		}
		rf.electionTimer = time.NewTimer(randomElectionTimeout())
		DPrintf("Term %03d: Peer %03d voted for Peer %03d\n", rf.currentTerm, rf.me, rf.votedFor)
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	DPrintf("Term %03d: Peer %03d against Peer %03d, already voted for %03d\n", rf.currentTerm, rf.me, args.CandidateID, rf.votedFor)

}

func randomElectionTimeout() time.Duration {
	amount := (rand.Int63() % 300) + 300
	return time.Duration(amount) * time.Millisecond
}
