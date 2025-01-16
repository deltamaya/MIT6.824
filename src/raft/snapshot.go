package raft

import (
	"time"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if rf.killed() {
		return
	}
	DPrintf("Peer: %03d Snap shot called: %d\n", rf.me, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.prevSnapshotIndex {
		return
	}
	rf.log = rf.log[rf.logIndexAbs(index):]

	rf.prevSnapshotIndex = index
	rf.prevSnapshotTerm = rf.log[rf.logIndexAbs(index)].Term
	rf.currentSnapshot = snapshot

	rf.log[0].Command = nil
	rf.persist()
}

func (rf *Raft) logIndexAbs(idx int) int {
	return idx - rf.prevSnapshotIndex
}

func (rf *Raft) logLengthAbs() int {
	return len(rf.log) + rf.prevSnapshotIndex
}

func (rf *Raft) getSnapshotData() []byte {
	return rf.persister.ReadSnapshot()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Done              bool
	// Offset            int
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
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
			ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
		DPrintf("Term %03d Peer %03d Install Snapshot to %03d timeout\n", args.Term, rf.me, server)
		stopCh <- struct{}{}
		return false
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if args.Term > rf.currentTerm {
		if rf.identity == LEADER {
			rf.leaderToFollower()
		}
		rf.identity = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.electionTimer = time.NewTimer(randomElectionTimeout())
		rf.currentSnapshot = args.Data

		rf.prevSnapshotIndex = args.LastIncludedIndex
		rf.prevSnapshotTerm = args.LastIncludedTerm
		rf.log = []LogEntry{
			{},
		}
		rf.currentSnapshot = args.Data
		rf.persist()
	}

	if args.LastIncludedIndex < rf.prevSnapshotIndex {
		return
	}
	DPrintf("Term %03d Peer %03d installed snapshot\n", rf.currentTerm, rf.me)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		SnapshotTerm:  args.Term,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshotToPeer(idx int) {
	rf.mu.Lock()
	DPrintf("sending install snapshot to %03d\n", idx)

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.prevSnapshotIndex,
		LastIncludedTerm:  rf.prevSnapshotTerm,
		Data:              rf.currentSnapshot,
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(idx, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	if args.LastIncludedIndex > rf.matchIndex[idx] {
		rf.matchIndex[idx] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[idx] {
		rf.nextIndex[idx] = args.LastIncludedIndex + 1
	}
	rf.mu.Unlock()
}
