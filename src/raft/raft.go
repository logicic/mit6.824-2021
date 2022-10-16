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
	voteCount int

	// volatile state on leaders
	// reinitialized after election
	nextIndex []int // for each server, index of next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to leader last log index+1)
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
	fmt.Printf("leader %d send heartbeat to %d in term %d at %v\n", args.LeaderId, rf.me, args.Term, time.Now())
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
		rf.role = 2
		fmt.Printf("AppendEntries :%d become follower!\n", rf.me)
	}

	reply.Success = true
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
	fmt.Printf("!!!!call request vote! %d vote to %d!\n",rf.me , args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm >= args.Term {
		fmt.Printf("%d's current term is %d, remote %d term is %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}
	if rf.votedFor > -1{
		fmt.Printf("%d has voted %d\n", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}
	// if rf.role == 1 {
	// 	// this server has vote to itself
	// 	fmt.Printf("%d has voted itself\n", rf.me)
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = false
	// 	return 
	// }
	if rf.role == 0 {
		// update role 
		fmt.Printf("RequestVote :%d become follower!\n", rf.me)
		rf.role = 2
	}
	// if rf.role != 2 {
	// 	fmt.Printf("%d's role is %d\n", rf.me, rf.role)
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = false
	// 	return 
	// }
	fmt.Printf("call request vote! %d vote to %d!\n",rf.me , args.CandidateId)
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
		fmt.Printf("%d term:%d, role:%d\n", rf.me, rf.currentTerm, rf.role)
		rf.mu.Unlock()
		voteCountSuccess := int64(1)	// vote to myself
		voteCountFailed := int64(0)
		for peerIndex := range rf.peers{
			if peerIndex == rf.me {
				continue
			}
			args := &RequestVoteArgs{
				Term: currentTerm,
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}
			go func(pindex int){
				if rf.sendRequestVote(pindex, args, reply){
					if reply.VoteGranted {
						// vote to me
						atomic.AddInt64(&voteCountSuccess,1)
						
					} else {
						atomic.AddInt64(&voteCountFailed,1)
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
				}
			}(peerIndex)
		}
		requestTimeout := time.Now().Add(1000*time.Millisecond).Unix()
		for {
			if requestTimeout <= time.Now().Unix(){
				rf.mu.Lock()
				rf.votedFor = -1
				rf.mu.Unlock()
				fmt.Printf("%d requestTimeout:%v, now: %v\n",rf.me, requestTimeout, time.Now())
				break
			}
			if atomic.LoadInt64(&voteCountSuccess) >= int64(rf.count/2+1){
				rf.mu.Lock()
				rf.votedFor = -1
				if 1 == rf.role && currentTerm == rf.currentTerm{
					// become leader
					fmt.Printf("%d become leader!\n", rf.me)
					rf.role = 0
					rf.mu.Unlock()
					go rf.doLeader()
				}else{
					rf.mu.Unlock()
				}
				break
			}
			if atomic.LoadInt64(&voteCountFailed) >= int64(rf.count/2+1) || atomic.LoadInt64(&voteCountSuccess) + atomic.LoadInt64(&voteCountFailed) >= int64(rf.count){
				// elect failed
				fmt.Printf("%d elect failed! retry!\n", rf.me)
				rf.mu.Lock()
				rf.votedFor = -1
				rf.mu.Unlock()
				break
			}
			time.Sleep(1*time.Millisecond)
		}
	}else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) doLeader(){
	// leader
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	fmt.Printf("%d current term :%d current role :%d\n", rf.me, rf.currentTerm, rf.role)
	rf.mu.Unlock()
	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}
		go func(pindex int){
			args := &AppendEntriesArgs{
				Term: currentTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(pindex, args, reply)
			fmt.Printf("%d sendAppendEntries to %d is %v\n", rf.me, pindex, ok)
			if ok {
				if reply.Term > currentTerm{
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = 2
						fmt.Printf("leader :%d become follower!\n", rf.me)
					}
					rf.mu.Unlock()
				}
			}
		}(peerIndex)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	MeanArrivalTime := 200
	heartbeat := 100
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if _, isLeader := rf.GetState(); !isLeader{
			electionTimeout := rand.Intn(300)+MeanArrivalTime
			fmt.Printf("%d's random time is %v\n", rf.me, electionTimeout)
			select {
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):	
				go rf.doCandidate()
			case <-rf.heartbeatCh:
				fmt.Printf("%d recv heartbeat!\n", rf.me)
			}
		}else {
			rf.doLeader()
			time.Sleep(time.Duration(heartbeat)*time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatCh = make(chan struct{},100)
	rf.votedFor = -1
	rf.role = 2 // follower
	rf.count = len(peers)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
