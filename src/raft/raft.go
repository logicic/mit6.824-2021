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
	"bytes"
	"fmt"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
	currentTerm int // latest term server has been(initialized to 0 on first boot, increase monotonically)
	votedFor    int // candidateId that received vote in current term(or null if none)
	// logEntries  []LogEntry // log entries
	logEntries Log
	role       int // 0:leader, 1:candidate, 2:follower
	count      int

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(initalized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initalized to 0, increases monotonically)
	heartbeatCh chan struct{}

	// volatile state on leaders
	// reinitialized after election
	nextIndex  []int // for each server, index of next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to leader last log index+1)
	applyCh    chan ApplyMsg

	applyCond *sync.Cond
	snaping   bool
}

const (
	NONE      = -1
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

// each entry contains command for state machine, and term when entry was received by leader
// type LogEntry struct {
// 	Command interface{}
// 	Term    int
// 	Index   int
// }

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.role == LEADER
	term = rf.currentTerm
	log.Errorf("%d get state: term is %d, role:%d\n", rf.me, rf.currentTerm, rf.role)
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log Log
	var votedFor int
	var currentTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode((&log)) != nil {
		fmt.Printf("%d Decode err", rf.me)
		return
	} else {
		rf.logEntries = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = log.Index0 - 1
	}
}

func (rf *Raft) readSnapshot(data []byte) []Entry {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []Entry
	if d.Decode((&log)) != nil {
		fmt.Printf("%d Decode err", rf.me)
		return nil
	} else {
		return log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.Snapshot(lastIncludedIndex, snapshot)
	fmt.Printf("%d CondInstallSnapshot!!\n", rf.me)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	fmt.Printf("%d snapshot[%d] !!!!!\n", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update logEntries, cut off it
	if rf.logEntries.Index0 >= index+1 {
		return
	}
	term := rf.logEntries.at(index).Term
	snapshotSlice := rf.logEntries.slice(index + 1)
	rf.logEntries.setIndex0(index+1, term)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	if snapshot != nil || len(snapshot) > 0 {
		temp := rf.persister.ReadSnapshot()
		snapData := rf.readSnapshot(temp)
		fmt.Printf("%d snapData:%v\n", rf.me, snapData)
		if snapData != nil && len(snapData) > 0 {
			snapshotIndex := len(snapData) - 1
			if snapshotIndex < index {
				snapData = append(snapData, snapshotSlice...)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(snapData)
				complateSnapshot := w.Bytes()
				rf.persister.SaveStateAndSnapshot(data, complateSnapshot)
				fmt.Printf("%d SaveStateAndSnapshot1 data%d\n", rf.me, snapData)
				return
			} else {
				rf.persister.SaveRaftState(data)
				fmt.Printf("%d SaveRaftState1 data%d\n", rf.me, rf.logEntries)
				return
			}
		} else {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(snapshotSlice)
			complateSnapshot := w.Bytes()
			rf.persister.SaveStateAndSnapshot(data, complateSnapshot)
			fmt.Printf("%d SaveStateAndSnapshot2 data%d\n", rf.me, snapData)
		}
		return
		// rf.persister.SaveStateAndSnapshot(data, append(temp, snapshot...))
	}
	rf.persister.SaveRaftState(data)
	fmt.Printf("%d SaveRaftState2 data%d\n", rf.me, rf.logEntries)
}

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

func (rf *Raft) updateTermWithoutLock(term int) {
	rf.votedFor = NONE
	rf.currentTerm = term
}

func (rf *Raft) updateRoleWithoutLock(role int) {
	if rf.role != role {
		rf.role = role
	}
}

type InstallSnapshotArgs struct {
	// Your data here (2D).
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Data              []byte
}

type InstallSnapshotReply struct {
	// Your data here (2D).
	Term int
}

func (rf *Raft) Installsnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d LastIncludedIndex:%d commitIndex:%d Index0:%d lastApplied:%d\n", rf.me, args.LastIncludedIndex, rf.commitIndex, rf.logEntries.Index0, rf.lastApplied)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.heartbeatCh <- struct{}{}
	rf.updateTermWithoutLock(args.Term)
	rf.updateRoleWithoutLock(FOLLOWER)

	if rf.snaping {
		return
	}

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if len(args.Data) <= 0 {
		return
	}
	tmpLog := rf.readSnapshot(args.Data)
	if tmpLog == nil {
		return
	}

	rf.logEntries.Entries = tmpLog[rf.logEntries.Index0:]
	if len(rf.logEntries.Entries) == 0 || rf.logEntries.Entries[0].Index <= rf.lastApplied {
		return
	}
	rf.snaping = true
	fmt.Printf("%d recv snapshot[%d-%d]\n", rf.me, rf.logEntries.Entries[0].Index, rf.logEntries.Entries[len(rf.logEntries.Entries)-1].Index)
	for _, log := range rf.logEntries.Entries {
		if log.Index < rf.logEntries.Index0 || log.Index < rf.lastApplied {
			continue
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		v := log.Command
		e.Encode(v)

		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      w.Bytes(),
			SnapshotTerm:  log.Term,
			SnapshotIndex: log.Index,
		}
		rf.lastApplied = log.Index
		fmt.Printf("%d applysnapshot1 %v apply: %v\n", rf.me, log.Command, applyMsg)
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
		fmt.Printf("%d applysnapshot2 %v apply: %v\n", rf.me, log.Command, applyMsg)
	}
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = min(rf.logEntries.len(), args.LastIncludedIndex)
		fmt.Printf("%d have updated commitIndex into %d\n", rf.me, rf.commitIndex)
	}
	rf.snaping = false
	// rf.commitIndex = args.LastIncludedIndex
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Installsnapshot", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d will recv %d logs:%v prevIndex:%d prevTerm:%d rf.term:%d args.term:%d\n", rf.me, args.LeaderId, args.LogEntries, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm, args.Term)
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

	// fmt.Printf("%d will recv %d logs:%v prevIndex:%d prevTerm:%d rf.term:%d args.term:%d\n", rf.me, args.LeaderId, args.LogEntries, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm, args.Term)
	// append logentry
	if args.PrevLogIndex > rf.logEntries.len()-1 || rf.logEntries.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		fmt.Printf("%d append leader %d prevLogIndex:%d prevLogTerm:%d\n", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		if args.PrevLogIndex > rf.logEntries.len()-1 {
			reply.RealNextIndex = rf.logEntries.len() - 1
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
			if rf.logEntries.len()-1 >= index && rf.logEntries.at(index).Term != v.Term {
				// overwrite existed log
				// rf.logEntries = rf.logEntries[:index]
				rf.logEntries.truncate(index)
				rf.persist()
			}
			if rf.logEntries.len()-1 < index {
				// append new log
				// rf.logEntries = append(rf.logEntries, args.LogEntries[i:]...)
				rf.logEntries.append(args.LogEntries[i:]...)
				rf.persist()
				break
			}
		}
		fmt.Printf("%d term:%d logs:%v\n", rf.me, rf.currentTerm, rf.logEntries)
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.logEntries.len()-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.logEntries.len() - 1
		}
	}

	// rf.checkLogEntries()
	rf.applyCond.Signal()
	// rf.persist()
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
	fmt.Printf("!!!!call request vote! %d vote to %d!rf.term:%d args.term:%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	if rf.currentTerm >= args.Term {
		log.Errorf("%d's current term is %d, remote %d term is %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if rf.currentTerm < args.Term {
		rf.updateTermWithoutLock(args.Term)
		// rf.updateRoleWithoutLock(FOLLOWER)
		fmt.Printf("RequestVote :%d become follower!\n", rf.me)
		if rf.role == LEADER {
			// update role
			log.Errorf("RequestVote :%d become follower!\n", rf.me)
			rf.role = FOLLOWER
		}
	}
	if rf.votedFor != NONE {
		log.Errorf("%d has voted %d\n", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	currentLastLogEntries := rf.logEntries.len() - 1
	currentLastEntryTerm := rf.logEntries.at(currentLastLogEntries).Term
	if currentLastLogEntries != 0 {
		if currentLastEntryTerm < args.LastLogTerm ||
			(currentLastEntryTerm == args.LastLogTerm && currentLastLogEntries <= args.LastLogIndex) {
			fmt.Printf("diao %d vote to %d but currentLastLogEntries:%d, currentLastEntryTerm:%d, args.LastLogTerm:%d, args.LastLogIndex:%d\n", rf.me, args.CandidateId,
				currentLastLogEntries, currentLastEntryTerm, args.LastLogTerm, args.LastLogIndex)
		} else {
			// log.Infof("%d is new up-to-date thant %d\n", rf.me, args.CandidateId)
			fmt.Printf("%d vote to %d but currentLastLogEntries:%d, currentLastEntryTerm:%d, args.LastLogTerm:%d, args.LastLogIndex:%d\n", rf.me, args.CandidateId,
				currentLastLogEntries, currentLastEntryTerm, args.LastLogTerm, args.LastLogIndex)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else {
		fmt.Printf("%d diao!\n", rf.me)
	}

	fmt.Printf("call request vote! %d vote to %d!\n", rf.me, args.CandidateId)
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

	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	preIndex := len(rf.logEntries.Entries) + rf.logEntries.Index0
	logItem := Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   preIndex,
	}
	rf.logEntries.append(logItem)
	rf.persist()
	term = rf.currentTerm
	index = preIndex
	rf.matchIndex[rf.me] = index
	fmt.Printf("%d temr[%d] append logs in start log[%d]:%v\n", rf.me, rf.currentTerm, index, rf.logEntries.at(index))
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
		log.Errorf("%d term:%d, role:%d\n", rf.me, rf.currentTerm, rf.role)
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
						fmt.Printf("%d become leader!At:%d\n", rf.me, rf.currentTerm)
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

func (rf *Raft) doLeader() {
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
			lastLogIndex := rf.logEntries.len() - 1
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
					fmt.Printf("send Installsnapshot rpc!\n")
					index0 := rf.logEntries.Index0
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
				log.Infof("%d lastLogIndex:%d nextIndex:%d", rf.me, lastLogIndex, nextIndex)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.updateRoleWithoutLock(FOLLOWER)
					fmt.Printf("who[%d] leader :%d become follower!\n", pindex, rf.me)
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
						fmt.Printf("%d append nextIndex[%d]:%v\n", rf.me, pindex, rf.nextIndex)

						rf.checkLogEntries()

					} else {
						// retry
						rf.nextIndex[pindex] = reply.RealNextIndex
						fmt.Printf("%d not append nextIndex[%d]:%v\n", rf.me, pindex, rf.nextIndex)

					}

				}
			}
		}(peerIndex)
	}
}

func (rf *Raft) doSnapshot(peer, logIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}

	snapDataByte := rf.persister.ReadSnapshot()
	if snapDataByte == nil || len(snapDataByte) == 0 {
		return
	}
	fmt.Printf("%d snapshot[%d]!!!!%d\n", rf.me, logIndex, peer)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: logIndex - 1,
		Data:              snapDataByte,
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	if rf.sendInstallSnapshot(peer, args, reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.updateTermWithoutLock(reply.Term)
			rf.updateRoleWithoutLock(FOLLOWER)
			return
		}
		rf.nextIndex[peer] = logIndex
		rf.matchIndex[peer] = logIndex - 1
		fmt.Printf("snapshot %d append nextIndex[%d]:%v\n", rf.me, peer, rf.nextIndex)
	} else {
		rf.mu.Lock()
	}

}

func (rf *Raft) checkLogEntries() {
	for i := rf.commitIndex + 1; i <= rf.logEntries.len()-1 && rf.role == LEADER; i++ {
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
			log.Errorf("%d rf.commitIndex:%d\n", rf.me, rf.commitIndex)
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.logEntries.len()-1 > rf.lastApplied && rf.lastApplied+1 >= rf.logEntries.Index0 {
			rf.lastApplied++
			fmt.Printf("%d term[%d] commitIndex[%d] lastLogIndex[%d] applyid logentry[%d].index0[%d]:%v\n", rf.me, rf.currentTerm, rf.commitIndex, rf.logEntries.len()-1, rf.lastApplied, rf.logEntries.Index0, rf.logEntries.at(rf.lastApplied))
			c := ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- c
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
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
		if _, isLeader := rf.GetState(); !isLeader {
			// electionTimeout := rand.Intn(300)+MeanArrivalTime
			electionTimeout := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(500) + 600
			log.Errorf("%d's random time is %v timeAt:%v\n", rf.me, electionTimeout, time.Now())
			select {
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
				rf.doCandidate()
			case <-rf.heartbeatCh:
				log.Debugf("%d recv heartbeat!\n", rf.me)
			}
		} else {
			rf.doLeader()
			if _, isLeader := rf.GetState(); isLeader {
				time.Sleep(100 * time.Millisecond)
			}
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
	rf.snaping = false
	rf.heartbeatCh = make(chan struct{}, 100)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.votedFor = NONE
	rf.role = FOLLOWER // follower
	rf.count = len(peers)
	rf.nextIndex = make([]int, rf.count)
	// rf.logEntries = make([]LogEntry, 1) //logEntries first index is 1
	rf.logEntries = makeEmptyLog()
	rf.logEntries.append(Entry{-1, 0, 0})
	rf.matchIndex = make([]int, rf.count)
	rf.lastApplied = 0
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Printf("%d begin raft term:%d log:%v\n", rf.me, rf.currentTerm, rf.logEntries)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()
	// go rf.syncLogEntries(applyCh)

	return rf
}
