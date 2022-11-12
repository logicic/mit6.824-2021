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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
		DPrintf("%d Decode err", rf.me)
		return
	} else {
		rf.logEntries = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = log.Index0 - 1
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.Snapshot(lastIncludedIndex, snapshot)
	DPrintf("%d CondInstallSnapshot!!\n", rf.me)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("%d snapshot[%d] !!!!!\n", rf.me, index)
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
		DPrintf("%d snapData:%v\n", rf.me, snapData)
		if snapData != nil && len(snapData) > 0 {
			snapshotIndex := len(snapData) - 1
			if snapshotIndex < index {
				snapData = append(snapData, snapshotSlice...)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(snapData)
				complateSnapshot := w.Bytes()
				rf.persister.SaveStateAndSnapshot(data, complateSnapshot)
				DPrintf("%d SaveStateAndSnapshot1 data%d\n", rf.me, snapData)
				return
			} else {
				rf.persister.SaveRaftState(data)
				DPrintf("%d SaveRaftState1 data%d\n", rf.me, rf.logEntries)
				return
			}
		} else {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(snapshotSlice)
			complateSnapshot := w.Bytes()
			rf.persister.SaveStateAndSnapshot(data, complateSnapshot)
			DPrintf("%d SaveStateAndSnapshot2 data%d\n", rf.me, snapData)
		}
		return
		// rf.persister.SaveStateAndSnapshot(data, append(temp, snapshot...))
	}
	rf.persister.SaveRaftState(data)
	DPrintf("%d SaveRaftState2 data%d\n", rf.me, rf.logEntries)
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
	DPrintf("%d temr[%d] append logs in start log[%d]:%v\n", rf.me, rf.currentTerm, index, rf.logEntries.at(index))
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

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.logEntries.len()-1 > rf.lastApplied && rf.lastApplied+1 >= rf.logEntries.Index0 {
			rf.lastApplied++
			DPrintf("%d term[%d] commitIndex[%d] lastLogIndex[%d] applyid logentry[%d].index0[%d]:%v\n", rf.me, rf.currentTerm, rf.commitIndex, rf.logEntries.len()-1, rf.lastApplied, rf.logEntries.Index0, rf.logEntries.at(rf.lastApplied))
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
			select {
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
				rf.doCandidate()
			case <-rf.heartbeatCh:
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
	DPrintf("%d begin raft term:%d log:%v\n", rf.me, rf.currentTerm, rf.logEntries)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()
	// go rf.syncLogEntries(applyCh)

	return rf
}
