package raft

import (
	"bytes"

	"6.824/labgob"
)

/*
2D lab其实做起来是有点疑惑的，snapshot是只存当前瞬间的最新的index logEntry？
这个怎么感觉怪怪的。然后我觉得应该是存全部才对呀。然后理论上一次installSnapshot RPC
请求只发送一个index，然后持续发送？感觉也不太对的样子？
不过我现在一次rpc发送全部的snapshot，应该不是对的感觉，没理由每次都发完全部。
之后可以改成leader.nextIndex[peerId]到leader.index0的切片发送到peer中，感觉这样比较合理把？
不知道我是不是哪里理解错了
*/

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.logEntries.getIndex0() >= lastIncludedIndex {
		return false
	}
	rf.logEntries = makeEmptyLog()
	rf.logEntries.setIndex0(lastIncludedIndex+1, lastIncludedTerm)
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	if snapshot != nil || len(snapshot) > 0 {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
	DPrintf("%d CondInstallSnapshot!!commitedId:%d lastapplied:%d log:%v\n", rf.me, rf.commitIndex, rf.lastApplied, rf.logEntries)
	return true
}

/*
这里的SnapShot()function 是snapshot所有index前（包括index）的logEntries
*/
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
	if rf.logEntries.getIndex0() >= index+1 {
		return
	}
	term := rf.logEntries.at(index).Term
	rf.logEntries.slice(index + 1)
	rf.logEntries.setIndex0(index+1, term)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	if snapshot != nil || len(snapshot) > 0 {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
	DPrintf("%d SaveRaftState2 data%d\n", rf.me, rf.logEntries)
}

func (rf *Raft) readSnapshot(data []byte) []Entry {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []Entry
	if d.Decode((&log)) != nil {
		DPrintf("%d Decode err", rf.me)
		return nil
	} else {
		return log
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
	DPrintf("%d LastIncludedIndex:%d commitIndex:%d Index0:%d lastApplied:%d\n", rf.me, args.LastIncludedIndex, rf.commitIndex, rf.logEntries.getIndex0(), rf.lastApplied)
	// 1. 判断任期
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	// 2. 发送installSnapshot也可以证明leader的存在
	rf.heartbeatCh <- struct{}{}
	rf.updateTermWithoutLock(args.Term)
	rf.updateRoleWithoutLock(FOLLOWER)

	// 3. 如果有正在处理的snapshot，一次只处理一个
	if rf.snaping {
		return
	}

	// 4. 判断传送过来的最新index是否已经在peer中了
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// 5. 数据判空
	if len(args.Data) <= 0 {
		return
	}
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// DPrintf("%d applysnapshot1 %v apply: %v\n", rf.me, log.Command, applyMsg)
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	rf.mu.Lock()
	// DPrintf("%d applysnapshot2 %v apply: %v\n", rf.me, log.Command, applyMsg)
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Installsnapshot", args, reply)
	return ok
}

func (rf *Raft) doSnapshot(peer int) {
	rf.mu.Lock()
	// 1. 再次确认是否还是leader
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}

	// 2. 读取所有的snapshot的数据0-index0
	snapDataByte := rf.persister.ReadSnapshot()
	if snapDataByte == nil || len(snapDataByte) == 0 {
		rf.mu.Unlock()
		return
	}

	logIndex0 := rf.logEntries.getIndex0()
	DPrintf("%d snapshot[%d]!!!!%d\n", rf.me, logIndex0, peer)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: logIndex0 - 1,
		LastIncludeTerm:   rf.logEntries.getTerm0(),
		Data:              snapDataByte,
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	// 3. 发送installsnapshot请求
	if rf.sendInstallSnapshot(peer, args, reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.updateTermWithoutLock(reply.Term)
			rf.updateRoleWithoutLock(FOLLOWER)
			rf.mu.Unlock()
			return
		}
		// 4. 更新nextIndex
		rf.nextIndex[peer] = logIndex0
		rf.matchIndex[peer] = logIndex0 - 1
		DPrintf("snapshot %d append nextIndex[%d]:%v\n", rf.me, peer, rf.nextIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}
