package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"fmt"
//	"6.824/labgob"
	"6.824/labrpc"
	log "github.com/sirupsen/logrus"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state on all servers
	currentTerm int  // latest term server has been(initialized to 0 on first boot, increase monotonically)
	votedFor int // candidateId that received vote in current term(or null if none)
	logEntries []LogEntry // log entries
	role int // 0:leader, 1:candidate, 2:follower
	count int

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(initalized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initalized to 0, increases monotonically)
	heartbeatCh chan struct{}
	leaderCh chan struct{}
	followerCh chan struct{}

	// volatile state on leaders
	// reinitialized after election
	nextIndex []int // for each server, index of next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to leader last log index+1)
	applyCh chan ApplyMsg 
}

// each entry contains command for state machine, and term when entry was received by leader
type LogEntry struct {
	Command interface{}
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	term = rf.currentTerm
	if rf.role == 0 {
		isleader = true
	}
	// fmt.Printf("%d get state: term is %d, role:%d\n", rf.me, rf.currentTerm, rf.role)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LogEntries []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	log.Infof("leader %d send heartbeat to %d in term %d at %v\n", args.LeaderId, rf.me, args.Term, time.Now())
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}
	rf.heartbeatCh <- struct{}{}
	rf.votedFor = -1
	rf.currentTerm = args.Term
	if rf.role != 2 {
		r := rf.role
		rf.role = 2
		if r == 0 {
			rf.followerCh<-struct{}{}
		}
		log.Debugf("AppendEntries :%d become follower!\n", rf.me)
	}

	// if args.LeaderCommit > rf.commitIndex{
	// 	rf.commitIndex = args.LeaderCommit
	// }

	reply.Success = true
	reply.Term = rf.currentTerm

	if len(args.LogEntries) > 0{
		log.Infof("%d will recv %d logs:%v prevIndex:%d prevTerm:%d", rf.me, args.LeaderId, args.LogEntries, args.PrevLogIndex, args.PrevLogTerm)
		// append logentry
		// if args.PrevLogIndex == 0 {
		// 	rf.logEntries = append(rf.logEntries, args.LogEntries...)
		// 	if args.LeaderCommit > rf.commitIndex{
		// 		rf.commitIndex = args.LeaderCommit
		// 	}
		// 	return
		// }
		if args.PrevLogIndex > len(rf.logEntries) -1 || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm{
			log.Infof("%d append leader %d prevLogIndex:%d prevLogTerm:%d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
			reply.Success = false
			return
		}

		if len(rf.logEntries)-1 == args.PrevLogIndex {
			rf.logEntries = append(rf.logEntries, args.LogEntries...)
		}else{
			// len(rf.logEntries)-1 > args.PrevLogIndex
			log.Warnf("%d cao!!! logs:%v",rf.me, rf.logEntries)
			for i :=0; i <= len(args.LogEntries)-1;i++{
				if i+args.PrevLogIndex+1<=len(rf.logEntries)-1 {
					if rf.logEntries[args.PrevLogIndex+1+i].Term != args.LogEntries[i].Term{
						rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+i+1], args.LogEntries[i:]...)				
						break
					}
				}else {
					rf.logEntries = append(rf.logEntries, args.LogEntries[i:]...)				
					break
				}
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex{
		if args.LeaderCommit < len(rf.logEntries)-1{
			rf.commitIndex = args.LeaderCommit
		}else{
			rf.commitIndex = len(rf.logEntries)-1
		}
	}
	rf.checkLogEntries()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 
	CandidateId int 
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Errorf("!!!!call request vote! %d vote to %d!timeAt:%v\n",rf.me , args.CandidateId, time.Now())
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm >= args.Term {
		log.Debugf("%d's current term is %d, remote %d term is %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}
	if rf.votedFor > -1{
		log.Errorf("%d has voted %d\n", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}

	
	currentLastLogEntries := len(rf.logEntries)-1
	currentLastEntryTerm := rf.logEntries[currentLastLogEntries].Term
	if currentLastLogEntries != 0{
		if currentLastEntryTerm < args.LastLogTerm || 
		(currentLastEntryTerm == args.LastLogTerm && currentLastLogEntries <= args.LastLogIndex) {
	
		}else{
			// log.Infof("%d is new up-to-date thant %d\n", rf.me, args.CandidateId)
			log.Errorf("%d vote to %d but currentLastLogEntries:%d, currentLastEntryTerm:%d, args.LastLogTerm:%d, args.LastLogIndex:%d", rf.me, args.CandidateId,
			currentLastLogEntries, currentLastEntryTerm, args.LastLogTerm, args.LastLogIndex)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return 
		}
	}


	if rf.role == 0 {
		// update role 
		log.Debugf("RequestVote :%d become follower!\n", rf.me)
		rf.role = 2
		rf.followerCh<-struct{}{}
	}
	// if rf.role != 2 {
	// 	fmt.Printf("%d's role is %d\n", rf.me, rf.role)
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = false
	// 	return 
	// }
	log.Errorf("call request vote! %d vote to %d!\n",rf.me , args.CandidateId)
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


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if _,isLeader := rf.GetState(); !isLeader{
		// not leader
		return -1, -1,false
	}

	// leader operation
	rf.mu.Lock()
	logItem := LogEntry{
		Command: command,
		Term: rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, logItem)
	// fmt.Printf("%d append logs in start %v\n", rf.me, time.Now())
	index = len(rf.logEntries)-1
	term = rf.logEntries[index].Term
	// log.Infof("%d log entry: %v\n", rf.me, rf.logEntries)
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) doCandidate(){
	rf.mu.Lock()
	if rf.role != 0 {
		// candidate operation
		if rf.role == 2 {
			rf.role = 1
		}
		rf.currentTerm++
		currentTerm := rf.currentTerm
		rf.votedFor = rf.me			// vote to myself
		lastLogIndex := len(rf.logEntries)-1
		lastLogTerm := rf.logEntries[lastLogIndex].Term
		fmt.Printf("%d term:%d, role:%d\n", rf.me, rf.currentTerm, rf.role)
		rf.mu.Unlock()
		voteCountSuccess := int64(1)	// vote to myself
		voteCountFailed := int64(0)
		voteCh := make(chan struct{},10)
		for peerIndex := range rf.peers{
			if peerIndex == rf.me {
				continue
			}
			args := &RequestVoteArgs{
				Term: currentTerm,
				CandidateId: rf.me,
				LastLogIndex:lastLogIndex,
				LastLogTerm:lastLogTerm,
			}
			reply := &RequestVoteReply{}
			go func(pindex int){
				if rf.sendRequestVote(pindex, args, reply){
					if reply.VoteGranted {
						// vote to me
						atomic.AddInt64(&voteCountSuccess,1)
						voteCh<-struct{}{}
					} else {
						atomic.AddInt64(&voteCountFailed,1)
						voteCh<-struct{}{}
					}
					if reply.Term > currentTerm{
						// double check
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
						}
						rf.mu.Unlock()
					}
				}else {
					atomic.AddInt64(&voteCountFailed,1)
					voteCh<-struct{}{}
				}
			}(peerIndex)
		}
		// requestTimeout := time.Now().Add(1000*time.Millisecond).Unix()
		spinCount := 0
		newTimer := time.NewTimer(1000*time.Millisecond)
		// s := time.Now()
		for {
			if spinCount > -1 {
				select {
				case <-voteCh:
					break
				case <-newTimer.C:
					rf.mu.Lock()
					rf.votedFor = -1
					rf.mu.Unlock()
					// fmt.Printf("%d 123spinCount:%d spend: %v\n",rf.me,spinCount, time.Since(s))
					return
				}
			}
			if atomic.LoadInt64(&voteCountSuccess) >= int64(rf.count/2+1){
				rf.mu.Lock()
				rf.votedFor = -1
				if 1 == rf.role && currentTerm == rf.currentTerm{
					// become leader
					fmt.Printf("%d become leader!At:%v\n", rf.me,time.Now())
					rf.role = 0
					rf.leaderCh<-struct{}{}
					rf.mu.Unlock()
					go rf.doLeader()
				}else{
					rf.mu.Unlock()
				}
				break
			}
			if atomic.LoadInt64(&voteCountFailed) >= int64(rf.count/2+1) || atomic.LoadInt64(&voteCountSuccess) + atomic.LoadInt64(&voteCountFailed) >= int64(rf.count){
				// elect failed
				log.Debugf("%d elect failed! retry!\n", rf.me)
				rf.mu.Lock()
				rf.votedFor = -1
				rf.mu.Unlock()
				break
			}
			// time.Sleep(1*time.Millisecond)
			spinCount++
		}
		// fmt.Printf("%d spinCount:%d spend: %v\n",rf.me,spinCount, time.Since(s))
	}else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) doLeader(){
	// leader
	// rf.mu.Lock()


	// // log.Infof("%d rf.logEntries:%v",rf.me ,rf.logEntries)
	// log.Debugf("%d current term :%d current role :%d\n", rf.me, rf.currentTerm, rf.role)
	// rf.mu.Unlock()
	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}

		go func(pindex int){
			var args AppendEntriesArgs
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			lastLogIndex := len(rf.logEntries)-1
			nextIndex := rf.nextIndex[pindex]
			commitIndex := rf.commitIndex
			if lastLogIndex >= nextIndex {
				logs := rf.logEntries[nextIndex:]
				term := rf.logEntries[nextIndex-1].Term
				rf.mu.Unlock()
				args = AppendEntriesArgs{
					Term: currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: nextIndex-1,
					PrevLogTerm: term,
					LogEntries:logs,
					LeaderCommit: commitIndex,
				}
				// log.Warnf("%d send heartbeat!!!!nextIndex[%d]:%d log:%v", rf.me,pindex,nextIndex, args)
				// commands = rf.logEntries[rf.nextIndex[pindex]:]
			}else {
				rf.mu.Unlock()
				args = AppendEntriesArgs{
					Term: currentTerm,
					LeaderId: rf.me,
					LeaderCommit: commitIndex,
				}
			}

			reply := &AppendEntriesReply{}
			s := time.Now()
			ok := rf.sendAppendEntries(pindex, &args, reply)
			elapsed := time.Since(s)
			log.Errorf("%d sendAppendEntries to %d is %v nextIndex:%d lastLogIndex:%d spend %v", rf.me,pindex,ok,nextIndex,lastLogIndex ,elapsed)
			log.Debugf("%d sendAppendEntries to %d is %v\n", rf.me, pindex, ok)
			if ok {
				log.Infof("%d lastLogIndex:%d nextIndex:%d",rf.me, lastLogIndex, nextIndex)
				if reply.Term > currentTerm{
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = 2
						rf.followerCh<-struct{}{}
						log.Debugf("leader :%d become follower!\n", rf.me)
					}
					rf.mu.Unlock()
				}else if lastLogIndex >= nextIndex{
					// append
					rf.mu.Lock()
					if reply.Success{
						
						rf.nextIndex[pindex] = lastLogIndex+1
						rf.matchIndex[pindex] = lastLogIndex
						rf.nextIndex[rf.me] = lastLogIndex+1
						rf.matchIndex[rf.me] = lastLogIndex
						rf.checkLogEntries()
						log.Errorf("%d matchIndex[%d]: %d",  rf.me,pindex, rf.matchIndex[pindex])
					}else {
						// retry
						rf.nextIndex[pindex]--
					}
					rf.mu.Unlock()
				}
			}
		}(peerIndex)
	}
}

func (rf *Raft) checkLogEntries(){
	for i := rf.commitIndex + 1; i <= len(rf.logEntries)-1;i++{
		majority := 0 
		for pindex := range rf.peers{
			if rf.matchIndex[pindex] >= i {
				majority++
			}
		}
		if majority >= len(rf.peers)/2+1{
			rf.commitIndex = i
			log.Errorf("%d rf.commitIndex:%d\n",rf.me, rf.commitIndex)
		}
	}
	// if rf.role == 0 {
	// 	go rf.doLeader()
	// }
	for rf.commitIndex > rf.lastApplied{
		log.Errorf("%d:%v",rf.me,time.Now())
		rf.lastApplied++
		log.Infof("%d applyid: %d logentry:%d", rf.me,rf.lastApplied,rf.logEntries)
		c := ApplyMsg{
			CommandValid : true,
			Command : rf.logEntries[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- c
	}
}

func (rf *Raft) syncLogEntries(applyCh chan ApplyMsg) {
	for rf.killed() == false{
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		log.Errorf("syncLogEntries %d:%v",rf.me,time.Now())
		for i := rf.commitIndex + 1; i <= len(rf.logEntries)-1;i++{
			majority := 0 
			for pindex := range rf.peers{
				if rf.matchIndex[pindex] >= i {
					majority++
				}
			}
			if majority >= len(rf.peers)/2+1{
				log.Warnf("%d rf.commitIndex:%d\n",rf.me, rf.commitIndex)
				rf.commitIndex = i
			}
		}

		for rf.commitIndex > rf.lastApplied{
			log.Errorf("%d:%v",rf.me,time.Now())
			rf.lastApplied++
			log.Infof("%d applyid: %d logentry:%d", rf.me,rf.lastApplied,rf.logEntries)
			c := ApplyMsg{
				CommandValid : true,
				Command : rf.logEntries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}

			log.Warnf("%d rf.lastApplied:%d ApplyMsg:%v",rf.me ,rf.lastApplied, c)
			applyCh<-c
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLog(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(1*time.Millisecond)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied{
			log.Errorf("%d:%v",rf.me,time.Now())
			rf.lastApplied++
			log.Infof("%d applyid: %d logentry:%d", rf.me,rf.lastApplied,rf.logEntries)
			c := ApplyMsg{
				CommandValid : true,
				Command : rf.logEntries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			log.Warnf("%d rf.lastApplied:%d ApplyMsg:%v",rf.me ,rf.lastApplied, c)
			applyCh<-c
		}else{
			rf.mu.Unlock()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// MeanArrivalTime := 200
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if _, isLeader := rf.GetState(); !isLeader{
			// electionTimeout := rand.Intn(300)+MeanArrivalTime
			electionTimeout := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(300)+200
			fmt.Printf("%d's random time is %v timeAt:%v\n", rf.me, electionTimeout, time.Now())
			select {
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):	
				go rf.doCandidate()
			case <-rf.heartbeatCh:
				log.Debugf("%d recv heartbeat!\n", rf.me)
			}
		}else{
			// time.Sleep(500*time.Millisecond)
			// followerCh block leader role
			<-rf.followerCh
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetLevel(log.PanicLevel)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatCh = make(chan struct{},100)
	rf.leaderCh = make(chan struct{},100)
	rf.followerCh = make(chan struct{},100)
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.role = 2 // follower
	rf.count = len(peers)
	rf.nextIndex = make([]int, rf.count)
	rf.logEntries = make([]LogEntry,1) 		//logEntries first index is 1
	lastLogIndex := len(rf.logEntries)-1
	for i:=range rf.nextIndex{
		// (initialized to leader last log index + 1
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, rf.count)
	rf.lastApplied = 0
	rf.commitIndex = 0	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.syncLogEntries(applyCh)
	// go rf.applyLog(applyCh)
	go func(){
		for rf.killed() == false {
			if _, isLeader := rf.GetState(); isLeader{
				rf.doLeader()
				time.Sleep(100*time.Millisecond)
			}else {
				// time.Sleep(500*time.Millisecond)
				// leaderCh block follower role
				<-rf.leaderCh
			}
		}
	}()
	// go func(){
	// 	for rf.killed() == false {
	// 		select{
	// 			case c:=<-rf.applyCh:
	// 				applyCh<-c
	// 		}
	// 	}
	// }()
	return rf
}
