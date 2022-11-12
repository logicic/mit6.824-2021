package raft

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []Entry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term          int
	Success       bool
	RealNextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d will recv %d logs:%v prevIndex:%d prevTerm:%d rf.term:%d args.term:%d\n", rf.me, args.LeaderId, args.LogEntries, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm, args.Term)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// refresh electionTimer
	rf.heartbeatCh <- struct{}{}
	rf.updateTermWithoutLock(args.Term)
	rf.updateRoleWithoutLock(FOLLOWER)
	if rf.snaping {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.PrevLogIndex < 0 {
		reply.Success = false
		reply.RealNextIndex = NONE
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// DPrintf("%d will recv %d logs:%v prevIndex:%d prevTerm:%d rf.term:%d args.term:%d\n", rf.me, args.LeaderId, args.LogEntries, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm, args.Term)
	// append logentry
	if args.PrevLogIndex > rf.logEntries.lastIndex() || rf.logEntries.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf("%d append leader %d prevLogIndex:%d prevLogTerm:%d\n", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		if args.PrevLogIndex > rf.logEntries.lastIndex() {
			reply.RealNextIndex = rf.logEntries.lastIndex()
		} else {
			conflictTerm := rf.logEntries.at(args.PrevLogIndex).Term
			reply.RealNextIndex = 1
			for i := args.PrevLogIndex - 1; i > 0; i-- {
				if rf.logEntries.at(i).Term != conflictTerm {
					reply.RealNextIndex = i + 1
					break
				}
			}
		}
		if reply.RealNextIndex == 0 {
			reply.RealNextIndex = 1
		}
		return
	}

	if len(args.LogEntries) > 0 {
		for i, v := range args.LogEntries {
			index := args.PrevLogIndex + 1 + i
			if rf.logEntries.lastIndex() >= index && rf.logEntries.at(index).Term != v.Term {
				// overwrite existed log
				// rf.logEntries = rf.logEntries[:index]
				rf.logEntries.truncate(index)
				rf.persist()
			}
			if rf.logEntries.lastIndex() < index {
				// append new log
				// rf.logEntries = append(rf.logEntries, args.LogEntries[i:]...)
				rf.logEntries.append(args.LogEntries[i:]...)
				rf.persist()
				break
			}
		}
		DPrintf("%d term:%d logs:%v\n", rf.me, rf.currentTerm, rf.logEntries)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logEntries.lastIndex())
	}

	rf.applyCond.Signal()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntries() {
	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}

		go func(pindex int) {
			var args AppendEntriesArgs
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			currentTerm := rf.currentTerm
			lastLogIndex := rf.logEntries.lastIndex()
			nextIndex := rf.nextIndex[pindex]
			commitIndex := rf.commitIndex
			preterm := rf.logEntries.Term0
			if nextIndex-1 >= 0 {
				preterm = rf.logEntries.at(nextIndex - 1).Term
			}
			var logs []Entry
			if lastLogIndex >= nextIndex {
				// logs = rf.logEntries[nextIndex:]
				logs = make([]Entry, lastLogIndex-nextIndex+1)
				if temp := rf.logEntries.move(nextIndex, lastLogIndex+1); temp != nil {
					copy(logs, temp)
				} else {
					DPrintf("send Installsnapshot rpc!\n")
					index0 := rf.logEntries.getIndex0()
					rf.mu.Unlock()
					rf.doSnapshot(pindex, index0)
					return
				}

				args = AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  preterm,
					LogEntries:   logs,
					LeaderCommit: commitIndex,
				}
			} else {
				args = AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: commitIndex,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  preterm,
				}
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(pindex, &args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.updateRoleWithoutLock(FOLLOWER)
					DPrintf("who[%d] leader :%d become follower!\n", pindex, rf.me)
					return
				}
				if rf.role == LEADER && reply.Term == rf.currentTerm {

					// append
					if reply.RealNextIndex == NONE {
						// out-dated reply
						return
					}
					if reply.Success {
						rf.nextIndex[pindex] = nextIndex + len(logs)
						rf.matchIndex[pindex] = nextIndex + len(logs) - 1
						DPrintf("%d append nextIndex[%d]:%v\n", rf.me, pindex, rf.nextIndex)

						rf.checkLogEntries()

					} else {
						// retry
						rf.nextIndex[pindex] = reply.RealNextIndex
						DPrintf("%d not append nextIndex[%d]:%v\n", rf.me, pindex, rf.nextIndex)

					}

				}
			}
		}(peerIndex)
	}
}

func (rf *Raft) checkLogEntries() {
	for i := rf.commitIndex + 1; i <= rf.logEntries.lastIndex() && rf.role == LEADER; i++ {
		if rf.logEntries.at(i).Term != rf.currentTerm {
			continue
		}
		majority := 0
		for pindex := range rf.peers {
			if rf.matchIndex[pindex] >= i {
				majority++
			}
		}
		if majority >= len(rf.peers)/2+1 {
			rf.commitIndex = i
			rf.applyCond.Signal()
		}
	}
}
