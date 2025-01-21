package raft

import (
	"math/rand"
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ret chan<- *RequestVoteReply) {
	timeout := time.NewTimer(RPCTimeout)
	defer timeout.Stop()
	notify := make(chan *RequestVoteReply, 1)
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
				notify <- reply
				break
			}
		}
	}()
	select {
	case r := <-notify:
		ret <- r
	case <-timeout.C:
		DPrintf("Term %03d Peer %03d Request Vote to %03d timeout\n", args.Term, rf.me, server)
		stopCh <- struct{}{}
		ret <- &RequestVoteReply{}
	}
}

func (rf *Raft) requestElection() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	if rf.identity == LEADER {
		rf.mu.Unlock()
		return
	}

	// vote for myself by default
	rf.changeIdentity(CANDIDATE)
	rf.persist()
	DPrintf("Term %03d: Candidate %03d requesting election.\n", rf.currentTerm, rf.me)
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		CandidateID:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers)-1)

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(idx int) {
			ret := make(chan *RequestVoteReply, 1)
			reply := &RequestVoteReply{}
			rf.sendRequestVote(idx, &args, reply, ret)
			reply = <-ret
			voteCh <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.changeIdentity(FOLLOWER)
					rf.votedFor = -1
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(idx)
	}

	resCount := 1
	grantCount := 1
	total := len(rf.peers)
	for {
		granted := <-voteCh
		resCount++
		if granted {
			grantCount++
		}
		DPrintf("vote: %v, allCount: %v, resCount: %v, grantedCount: %v", granted, total, resCount, grantCount)

		if rf.isMajority(grantCount) {
			rf.mu.Lock()
			DPrintf("before try change to leader,count:%d, args:%+v, currentTerm: %v, argsTerm: %v", grantCount, args, rf.currentTerm, args.Term)
			if rf.identity == CANDIDATE && rf.currentTerm == args.Term {
				rf.changeIdentity(LEADER)
			}
			if rf.identity == LEADER {
				rf.resetAllAppendEntryTimerTrigger()
			}
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if resCount == total || rf.isMajority(resCount-grantCount) {
			return
		}
	}
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
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("Term %03d: Peer %03d against Peer %03d, low candidate term %03d\n", rf.currentTerm, rf.me, args.CandidateID, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// args.Term == rf.currentTerm
	lastLogIndex := rf.logLengthAbs() - 1
	lastLogTerm := rf.logs[rf.logIndexAbs(lastLogIndex)].Term
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
		rf.resetElectionTimer()
		if rf.identity == LEADER {
			rf.changeIdentity(FOLLOWER)
		}
		DPrintf("Term %03d: Peer %03d voted for Peer %03d\n", rf.currentTerm, rf.me, rf.votedFor)
		return
	}
	reply.VoteGranted = false
	DPrintf("Term %03d: Peer %03d against Peer %03d, already voted for %03d\n", rf.currentTerm, rf.me, args.CandidateID, rf.votedFor)

}

func randomElectionTimeout() time.Duration {
	amount := (rand.Int63() % 300) + 300
	return time.Duration(amount) * time.Millisecond
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) changeIdentity(ident int) {
	rf.identity = ident
	switch ident {
	case FOLLOWER:
	case CANDIDATE:
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()
	case LEADER:
		_, lastLogIndex := rf.lastLogTermIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = lastLogIndex
		}
		rf.resetElectionTimer()
	default:
		DPrintf("Invalid identity: %d\n", ident)
	}

}
