package raft

import (
	"time"
)

func (rf *Raft) getAppendEntries(peerID int) (int, int, []LogEntry) {
	nextIndex := rf.nextIndex[peerID]
	lastLogIndex := rf.logLengthAbs() - 1
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		prevLogIndex := lastLogIndex
		prevLogTerm := rf.logs[rf.logIndexAbs(prevLogIndex)].Term
		return prevLogIndex, prevLogTerm, []LogEntry{}
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.logs[rf.logIndexAbs(prevLogIndex)].Term

	entries := make([]LogEntry, lastLogIndex-nextIndex+1)
	copy(entries, rf.logs[nextIndex-rf.lastSnapshotIndex:])
	return prevLogIndex, prevLogTerm, entries
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply, ret chan<- *AppendEntryReply) {
	timeout := time.NewTimer(RPCTimeout)
	defer timeout.Stop()
	stopCh := make(chan struct{}, 1)
	notify := make(chan *AppendEntryReply, 1)
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
			ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
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
		DPrintf("Term %03d Peer %03d Append Entry to %03d timeout\n", args.Term, rf.me, server)
		stopCh <- struct{}{}
		ret <- &AppendEntryReply{}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(idx int) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	if rf.identity != LEADER {
		rf.resetAppendEntriesTimer(idx)
		rf.mu.Unlock()
		return
	}

	prevLogIndex, prevLogTerm, logs := rf.getAppendEntries(idx)
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntryReply{}
	rf.resetAppendEntriesTimer(idx)
	ret := make(chan *AppendEntryReply, 1)
	rf.mu.Unlock()

	rf.sendAppendEntry(idx, &args, reply, ret)

	rf.mu.Lock()
	reply = <-ret
	if reply.Term > rf.currentTerm {
		rf.changeIdentity(FOLLOWER)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.identity != LEADER || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		if reply.NextLogIndex > rf.nextIndex[idx] {
			rf.nextIndex[idx] = reply.NextLogIndex
			rf.matchIndex[idx] = reply.NextLogIndex - 1
		}
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
			hasCommit := false
			for i := rf.commitIndex + 1; i <= rf.matchIndex[idx]; i++ {
				count := 0
				for j := 0; j < len(rf.peers); j++ {
					if i <= rf.matchIndex[j] || j == rf.me {
						count++
					}
				}
				if rf.isMajority(count) {
					rf.commitIndex = i
					hasCommit = true
				} else {
					break
				}
			}
			if hasCommit {
				DPrintf("Term %03d Leader %03d update commit index -> %d\n", rf.currentTerm, rf.me, rf.commitIndex)
				rf.notifyApplyCh <- struct{}{}
			}
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if reply.NextLogIndex != 0 {
		if reply.NextLogIndex > rf.lastSnapshotIndex {
			rf.nextIndex[idx] = reply.NextLogIndex
			rf.resetAppendEntryTimerTrigger(idx)
		} else {
			go rf.sendInstallSnapshotToPeer(idx)
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	defer rf.persist()
	rf.changeIdentity(FOLLOWER)
	rf.resetElectionTimer()

	// if this is an outdated call
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}

	lastLogIndex := rf.logLengthAbs() - 1

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapshotIndex + 1
		DPrintf("Term %03d Peer %03d refuse newer snapshot: %d, prevLogIndex: %d\n", rf.currentTerm, rf.me, rf.lastSnapshotIndex, args.PrevLogIndex)
		DPrintf("Term %03d Peer %03d log: %v args: %v\n", rf.currentTerm, rf.me, rf.logs, args.Entries)
		return
	}

	// has log gap
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextLogIndex = lastLogIndex + 1
		DPrintf("Term %03d Peer %03d refuse ae log gap, prev index: %d, last: %d,next: %03d\n", rf.currentTerm, rf.me, args.PrevLogIndex, lastLogIndex, reply.NextLogIndex)
		DPrintf("Term %03d Peer %03d log: %v args: %v\n", rf.currentTerm, rf.me, rf.logs, args.Entries)
		return
	}

	// term mismatch
	if lastLogIndex != 0 && rf.logs[rf.logIndexAbs(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		index := args.PrevLogIndex
		for index > rf.commitIndex && rf.logs[rf.logIndexAbs(index)].Term == rf.logs[rf.logIndexAbs(args.PrevLogIndex)].Term {
			index--
		}
		reply.NextLogIndex = index + 1
		DPrintf("Term %03d Peer %03d refuse ae for term mismatch(%d!=%d), next: %03d\n", rf.currentTerm, rf.me, rf.logs[rf.logIndexAbs(args.PrevLogIndex)].Term, args.PrevLogTerm, reply.NextLogIndex)
		DPrintf("Term %03d Peer %03d logs: %v args: %v\n", rf.currentTerm, rf.me, rf.logs, args.Entries)
		return
	}

	// outdated append entry rpc, reply success
	lastLogTerm := rf.logs[rf.logIndexAbs(lastLogIndex)].Term
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	if lastLogTerm == args.Term && lastLogIndex > argsLastIndex {
		DPrintf("Term %03d Peer %03d received outdated RPC: lastLogIndex: %d, entryLastIndex: %d\n", rf.currentTerm, rf.me, lastLogIndex, argsLastIndex)
		reply.Success = false
		reply.NextLogIndex = 0
		return
	}

	if args.LeaderCommit > rf.commitIndex {
		idx := 0
		if lastLogIndex < args.LeaderCommit {
			idx = lastLogIndex
		} else {
			idx = args.LeaderCommit
		}
		rf.commitIndex = idx
		rf.notifyApplyCh <- struct{}{}
		DPrintf("Term %03d Follower %03d update commit to %03d\n", rf.currentTerm, rf.me, idx)
	}

	// matched, replicate logs
	rf.logs = append(rf.logs[:rf.logIndexAbs(args.PrevLogIndex+1)], args.Entries...)
	reply.Success = true
	reply.NextLogIndex = rf.logLengthAbs()
	DPrintf("Term %03d Peer %03d accepted ae, next: %03d\n", rf.currentTerm, rf.me, reply.NextLogIndex)
	DPrintf("Term %03d Peer %03d logs: %v args: %v\n", rf.currentTerm, rf.me, rf.logs, args.Entries)
}

func (rf *Raft) resetAllAppendEntryTimerTrigger() {
	for _, timer := range rf.appendEntriesTimers {
		timer.Stop()
		timer.Reset(0)
	}
}
func (rf *Raft) resetAppendEntryTimerTrigger(idx int) {
	rf.appendEntriesTimers[idx].Stop()
	rf.appendEntriesTimers[idx].Reset(0)
}

func (rf *Raft) resetAppendEntriesTimer(idx int) {
	rf.appendEntriesTimers[idx].Stop()
	rf.appendEntriesTimers[idx].Reset(HeartbeatInterval)
}
