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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("!!!!call request vote! %d vote to %d!rf.term:%d args.term:%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
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
	if rf.votedFor != NONE {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	currentLastLogEntries := rf.logEntries.len() - 1
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

	DPrintf("call request vote! %d vote to %d!\n", rf.me, args.CandidateId)
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

func (rf *Raft) doCandidate() {
	rf.mu.Lock()
	if rf.role != LEADER {
		// candidate operation
		rf.updateRoleWithoutLock(CANDIDATE)
		rf.updateTermWithoutLock(rf.currentTerm + 1)
		rf.votedFor = rf.me // vote to myself
		lastLogIndex := rf.logEntries.len() - 1
		lastLogTerm := rf.logEntries.at(lastLogIndex).Term
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
			LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
		gotVoted := 1
		rf.mu.Unlock()
		for peerIndex := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			go func(pindex int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(pindex, args, reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.updateTermWithoutLock(reply.Term)
						rf.updateRoleWithoutLock(FOLLOWER)
						rf.heartbeatCh <- struct{}{}
						rf.mu.Unlock()
						return
					}

					if rf.role != CANDIDATE || reply.Term != rf.currentTerm {
						// ignore outdated requestVoteReply
						rf.mu.Unlock()
						return
					}

					if reply.VoteGranted {
						gotVoted++
					}

					if gotVoted > len(rf.peers)/2 {
						DPrintf("%d become leader!At:%d\n", rf.me, rf.currentTerm)
						rf.updateRoleWithoutLock(LEADER)
						rf.votedFor = NONE
						rf.reInitializedLeaderStateWithoutLock()
						rf.mu.Unlock()
						rf.heartbeatCh <- struct{}{}
						go rf.doLeader()
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
		rf.nextIndex[i] = rf.logEntries.len() - 1 + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	// 设置自己的matchIndex
	rf.matchIndex[rf.me] = rf.logEntries.len() - 1

}
