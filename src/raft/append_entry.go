package raft

import (
	"time"
)

func (rf *Raft) getAppendEntries(peerID int) (int, int, []LogEntry) {
	nextIndex := rf.nextIndex[peerID]
	lastLogIndex := rf.logLengthAbs() - 1
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		prevLogIndex := lastLogIndex
		prevLogTerm := rf.log[rf.logIndexAbs(prevLogIndex)].Term
		return prevLogIndex, prevLogTerm, []LogEntry{}
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.log[rf.logIndexAbs(prevLogIndex)].Term

	entries := rf.log[rf.logIndexAbs(nextIndex):]
	return prevLogIndex, prevLogTerm, entries
}

func (rf *Raft) syncEntries() {
	if rf.killed() {
		return
	}
	if rf.identity != LEADER {
		return
	}
	rf.persist()
	defer rf.persist()
	DPrintf("Term %03d: Peer %03d syncing entires\n", rf.currentTerm, rf.me)
	reachable := 1
	allSuccess := true
	DPrintf("Leader log: %v\n", rf.log)
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			rf.mu.Lock()
			prevLogIndex, prevLogTerm, logs := rf.getAppendEntries(idx)
			args := AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      logs,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntryReply{}
			rf.mu.Unlock()
			ok := rf.sendAppendEntry(idx, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			reachable++
			if !reply.Success {
				allSuccess = false
				if reply.NextLogIndex > 0 {
					if reply.NextLogIndex > rf.lastSnapshotIndex {
						rf.nextIndex[idx] = reply.NextLogIndex
					} else {
						go rf.sendInstallSnapshotToPeer(idx)
					}
				}
			}

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
					}
				}
				if hasCommit {
					DPrintf("Term %03d Leader %03d update commit index -> %d\n", rf.currentTerm, rf.me, rf.commitIndex)
					rf.notifyApplyCh <- struct{}{}
				}
			}

		}(idx)
	}
	time.Sleep(50 * time.Millisecond)

	rf.mu.Lock()

	if rf.killed() {
		return
	}

	rf.electionTimer = time.NewTimer(randomElectionTimeout())

	if !rf.isMajority(reachable) {
		if rf.appendEntryRetry < 3 {
			rf.appendEntryRetry++
			rf.resetSyncTimeTrigger()
			return
		}
		DPrintf("Term %03d: Leader %03d didn't receive enough heartbeats: %d\n", rf.currentTerm, rf.me, reachable)
		rf.leaderToFollower()
		return
	}
	rf.appendEntryRetry = 0
	if allSuccess {
		rf.resetSyncTime()
	} else {
		rf.resetSyncTimeTrigger()
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
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
		stopCh <- struct{}{}
		return false
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	defer rf.persist()
	switch rf.identity {
	case FOLLOWER:
	case CANDIDATE:
		if args.Term >= rf.currentTerm {
			rf.identity = FOLLOWER
		}
	case LEADER:
		if args.Term > rf.currentTerm {
			DPrintf("Term %03d Leader %03d got AE from Peer %03d, term: %03d\n", rf.currentTerm, rf.me, args.LeaderID, args.Term)
			rf.leaderToFollower()
		}
	}

	// if this is an outdated call
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
	rf.electionTimer = time.NewTimer(randomElectionTimeout())

	lastLogIndex := rf.logLengthAbs() - 1

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapshotIndex + 1
		DPrintf("Term %03d Peer %03d refuse newer snapshot: %d, prevLogIndex: %d\n", rf.currentTerm, rf.me, rf.lastSnapshotIndex, args.PrevLogIndex)
		DPrintf("Term %03d Peer %03d log: %v args: %v\n", rf.currentTerm, rf.me, rf.log, args.Entries)
		return
	}

	// has log gap
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextLogIndex = lastLogIndex + 1
		DPrintf("Term %03d Peer %03d refuse ae log gap, prev index: %d, last: %d,next: %03d\n", rf.currentTerm, rf.me, args.PrevLogIndex, lastLogIndex, reply.NextLogIndex)
		DPrintf("Term %03d Peer %03d log: %v args: %v\n", rf.currentTerm, rf.me, rf.log, args.Entries)
		return
	}

	// term mismatch
	if lastLogIndex != 0 && rf.log[rf.logIndexAbs(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		index := args.PrevLogIndex
		for index > rf.commitIndex && rf.log[rf.logIndexAbs(index)].Term == rf.log[rf.logIndexAbs(args.PrevLogIndex)].Term {
			index--
		}
		reply.NextLogIndex = index + 1
		DPrintf("Term %03d Peer %03d refuse ae for term mismatch(%d!=%d), next: %03d\n", rf.currentTerm, rf.me, rf.log[rf.logIndexAbs(args.PrevLogIndex)].Term, args.PrevLogTerm, reply.NextLogIndex)
		DPrintf("Term %03d Peer %03d logs: %v args: %v\n", rf.currentTerm, rf.me, rf.log, args.Entries)
		return
	}

	// outdated append entry rpc, reply success
	lastLogTerm := rf.log[rf.logIndexAbs(lastLogIndex)].Term
	argsLastIndex := args.PrevLogIndex + 1 + len(args.Entries)
	if lastLogTerm == args.Term && rf.logLengthAbs() > argsLastIndex {
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
	rf.log = append(rf.log[:rf.logIndexAbs(args.PrevLogIndex+1)], args.Entries...)
	reply.Success = true
	reply.NextLogIndex = rf.logLengthAbs()
	DPrintf("Term %03d Peer %03d accepted ae, next: %03d\n", rf.currentTerm, rf.me, reply.NextLogIndex)
	DPrintf("Term %03d Peer %03d logs: %v args: %v\n", rf.currentTerm, rf.me, rf.log, args.Entries)
}

func (rf *Raft) resetSyncTime() {
	rf.syncEntriesTimer = time.NewTimer(HeartbeatInterval)
}
func (rf *Raft) resetSyncTimeTrigger() {
	rf.syncEntriesCh <- struct{}{}
	rf.syncEntriesTimer = time.NewTimer(HeartbeatInterval)
}
