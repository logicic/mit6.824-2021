package raft

/*
整个append log entry的过程是：
1. start() function append logEntry to leader's logEntry
2. logEntry.Len() >= nextIndex , append logEntry to peer
3. if ok, update 对应的nextIndex，matchIndex
3.1 if failed, update 对应的nextIndex，进行回退尝试逻辑
4. logEntry.Len() > commitedIndex, 触发matchIndex的绝大多数的比较
4.1 如果matchIndex获得绝大多数的认可，则说明当前commitedIndex可以更新
5. commitedIndex的更新，发送signal同步，appliedIndex的更新，appliedIndex更新则说明可以发送到state machine
*/

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
	// 1. 判定任期
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2. reset electionTimeout 更新相关状态
	// refresh electionTimer
	// rf.heartbeatCh <- struct{}{}
	rf.resetElectionTimer()
	rf.updateTermWithoutLock(args.Term)
	rf.updateRoleWithoutLock(FOLLOWER)
	// 3. 当处于installSnapshot的状态时，阻止进行append entries在心跳包的操作，简化logEntry的同步
	if rf.snaping {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 4. 判断preLogIndex
	if args.PrevLogIndex < 0 {
		reply.Success = false
		reply.RealNextIndex = NONE
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// 5. 判断得出prevLogIndex不符合或者prevLogTerm不匹配，进行一些操作找到比较贴近的realNextIndex告诉leader
	// 在进行下轮appendEntry的时候可以更快一些，在早的版本里是没有这个的，直接让leader的nextIndex--
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

	// 6. 一切都符合条件后，进行append，append又分为，替换和append两步
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

	// 7. follower的log apply是通过leader告诉follower，leader已经成功commit了，你也可以进行commit了
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logEntries.lastIndex())
		// 8. 更新好follower.commitIndex后，就可以通知follower apply新的logEnries了
		rf.applyCond.Signal()
	}

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
			// 1. double check 再次检查是否是leder，防止过期请求对当前状态影响
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			// 2. 准备参数
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
					rf.mu.Unlock()
					rf.doSnapshot(pindex)
					return
				}
			}
			args = AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  preterm,
				LogEntries:   logs,
				LeaderCommit: commitIndex,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			// 3. 发送到除了自己外的所有peer，leder并不知道谁在线还是不在线
			if rf.sendAppendEntries(pindex, &args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 3.1 处理任期问题
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.updateRoleWithoutLock(FOLLOWER)
					DPrintf("who[%d] leader :%d become follower!\n", pindex, rf.me)
					return
				}
				// 3.2 过期请求的判定
				if rf.role == LEADER && reply.Term == rf.currentTerm {

					// append
					if reply.RealNextIndex == NONE {
						// out-dated reply
						return
					}
					// 3.3 请求成功， 更新nextIndex，以驱动下一次的append
					if reply.Success {
						rf.nextIndex[pindex] = nextIndex + len(logs)
						rf.matchIndex[pindex] = nextIndex + len(logs) - 1
						DPrintf("%d append nextIndex[%d]:%v\n", rf.me, pindex, rf.nextIndex)

						rf.checkLogEntries()

					} else {
						// retry
						// 3.4 失败，log任期不对，进行回退and retry
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
