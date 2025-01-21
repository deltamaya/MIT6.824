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
	DPrintf("Peer: %03d SNAPSHOT called: %d\n", rf.me, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.lastSnapshotIndex {
		return
	}

	prev := rf.lastSnapshotIndex
	rf.lastSnapshotTerm = rf.logs[rf.logIndexAbs(index)].Term
	rf.lastSnapshotIndex = index
	rf.logs = rf.logs[index-prev:]
	rf.currentSnapshot = snapshot
	rf.logs[0].Term = rf.lastSnapshotTerm
	rf.logs[0].Command = nil
	rf.persist()
}

func (rf *Raft) logIndexAbs(idx int) int {
	return idx - rf.lastSnapshotIndex
}

func (rf *Raft) logLengthAbs() int {
	return len(rf.logs) + rf.lastSnapshotIndex
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, ret chan *InstallSnapshotReply) {
	timeout := time.NewTimer(RPCTimeout)
	defer timeout.Stop()
	notify := make(chan *InstallSnapshotReply, 1)
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
				notify <- reply
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case r := <-notify:
		ret <- r
	case <-timeout.C:
		DPrintf("Term %03d Peer %03d Install Snapshot to %03d timeout\n", args.Term, rf.me, server)
		stopCh <- struct{}{}
		ret <- nil
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("Term %03d Peer %03d received outdated snapshot term: %d\n", rf.currentTerm, rf.me, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		if rf.identity == LEADER {
			rf.changeIdentity(FOLLOWER)
		}
		rf.identity = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.electionTimer.Reset(randomElectionTimeout())
	}
	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		DPrintf("Term %03d Peer %03d received outdated snapshot index: %d, my: %d\n", rf.currentTerm, rf.me, args.LastIncludedIndex, rf.lastSnapshotIndex)
		return
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.currentSnapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.logs = []LogEntry{
		{Term: args.LastIncludedTerm},
	}

	rf.persist()
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
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.currentSnapshot,
	}
	rf.mu.Unlock()
	for !rf.killed() {
		reply := &InstallSnapshotReply{}
		ret := make(chan *InstallSnapshotReply, 1)
		rf.sendInstallSnapshot(idx, &args, reply, ret)
		reply = <-ret
		if reply == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.identity != LEADER || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.changeIdentity(FOLLOWER)
			rf.currentTerm = reply.Term
			rf.resetElectionTimer()
			rf.persist()
			return
		}

		if args.LastIncludedIndex > rf.matchIndex[idx] {
			rf.matchIndex[idx] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[idx] {
			rf.nextIndex[idx] = args.LastIncludedIndex + 1
		}
		return
	}
}
