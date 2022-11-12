package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

/*
Note:
在RequestVote function中添加了更严格的投票限制。
因为debug发现，有同一个任期，竟然有不同的serverID曾任leader。
错误原因：crash的节点在重启后，由与拿来的leader role变为
follower role，是平替的，即follower 的term和leder时的term时一样的，
由别的node提醒，即append enter，term不够，requestVote term不够，收到心跳包时
update term。那么，就会出现两个相同的term但command不同的log，由于判断是基于term，
其实按照raft的代码，这两个不同command的log都是合法的，但是是不同log。
或者是ABC三个server，A为leader，任期为8，刚竞选成功，还没来得及发送心跳包，挂掉了，BC仍然为7任期，follower，它们在选举timeout发生后，开始竞选，BC都是有可能成为leader的，假设B成为leader，那么任期为8，那么任期为8，历史上曾经是两个不同的server，这个应该是不合理的地方。
但是论文上写的确实是rf.currentTerm > args.Term，不投票，参考被别人的代码，好像这个case也会出现的样子，暂时想不通，我自己增加了这个限制，测试了100此2A 2B 2C都是通过的
*/
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("call1 request vote! %d vote to %d!rf.term:%d args.term:%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	// 1. 任期判断
	if rf.currentTerm >= args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if rf.currentTerm < args.Term {
		rf.updateTermWithoutLock(args.Term)
		// rf.updateRoleWithoutLock(FOLLOWER)
		DPrintf("RequestVote :%d become follower!\n", rf.me)
		if rf.role == LEADER {
			// update role
			rf.role = FOLLOWER
		}
	}
	// 2.是否已经投过票
	if rf.votedFor != NONE {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 3. paper5.4，投票限制，lastLogTerm是否大，等大的话需要比较长度
	currentLastLogEntries := rf.logEntries.lastIndex()
	currentLastEntryTerm := rf.logEntries.at(currentLastLogEntries).Term
	if currentLastLogEntries != 0 {
		if currentLastEntryTerm < args.LastLogTerm ||
			(currentLastEntryTerm == args.LastLogTerm && currentLastLogEntries <= args.LastLogIndex) {
			DPrintf("diao %d vote to %d but currentLastLogEntries:%d, currentLastEntryTerm:%d, args.LastLogTerm:%d, args.LastLogIndex:%d\n", rf.me, args.CandidateId,
				currentLastLogEntries, currentLastEntryTerm, args.LastLogTerm, args.LastLogIndex)
		} else {
			// log.Infof("%d is new up-to-date thant %d\n", rf.me, args.CandidateId)
			DPrintf("%d vote to %d but currentLastLogEntries:%d, currentLastEntryTerm:%d, args.LastLogTerm:%d, args.LastLogIndex:%d\n", rf.me, args.CandidateId,
				currentLastLogEntries, currentLastEntryTerm, args.LastLogTerm, args.LastLogIndex)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else {
		DPrintf("%d diao!\n", rf.me)
	}

	// 4. 完成投票
	DPrintf("call2 request vote! %d vote to %d!\n", rf.me, args.CandidateId)
	rf.heartbeatCh <- struct{}{}
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	if rf.role != LEADER {
		// candidate operation
		rf.updateRoleWithoutLock(CANDIDATE)
		rf.updateTermWithoutLock(rf.currentTerm + 1)
		rf.votedFor = rf.me // vote to myself
		lastLogIndex := rf.logEntries.lastIndex()
		lastLogTerm := rf.logEntries.at(lastLogIndex).Term
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
			LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
		gotVoted := 1 //  vote to myself
		rf.mu.Unlock()
		for peerIndex := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			go func(pindex int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(pindex, args, reply) {
					rf.mu.Lock()
					// 1. 检查是否任期是否符合要求
					if reply.Term > rf.currentTerm {
						rf.updateTermWithoutLock(reply.Term)
						rf.updateRoleWithoutLock(FOLLOWER)
						rf.heartbeatCh <- struct{}{}
						rf.mu.Unlock()
						return
					}

					// 2. 检查是否是过期的reply， 因为会有网络延迟等因素，需要代码确保还具有时效性
					if rf.role != CANDIDATE || reply.Term != rf.currentTerm {
						// ignore outdated requestVoteReply
						rf.mu.Unlock()
						return
					}

					// 3. 获得投票
					if reply.VoteGranted {
						gotVoted++
					}

					// 4. 判断是否达到绝大多数的投票
					/*
						4.1 update role to Leader
						4.2 reset votedFor
						4.3 reset nextIndex matchIndex
						4.4 reset election Timeout
						4.5 send heartbeat/appendEntries 宣告自己是leader
					*/
					if gotVoted > len(rf.peers)/2 {
						DPrintf("%d become leader!At:%d\n", rf.me, rf.currentTerm)
						rf.updateRoleWithoutLock(LEADER)
						rf.votedFor = NONE
						rf.reInitializedLeaderStateWithoutLock()
						rf.mu.Unlock()
						rf.heartbeatCh <- struct{}{}
						go rf.appendEntries()
					} else {
						rf.mu.Unlock()
					}
				}
			}(peerIndex)
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) reInitializedLeaderStateWithoutLock() {
	// reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logEntries.len()
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	// 设置自己的matchIndex
	rf.matchIndex[rf.me] = rf.logEntries.lastIndex()

}
