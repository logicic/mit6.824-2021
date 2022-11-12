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
	rf.Snapshot(lastIncludedIndex, snapshot)
	DPrintf("%d CondInstallSnapshot!!\n", rf.me)
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

	// 6. 解析传输过来的leader的snapshot数据
	tmpLog := rf.readSnapshot(args.Data)
	if tmpLog == nil {
		return
	}

	// 7. 替换
	rf.logEntries.modify(tmpLog[rf.logEntries.Index0:])
	// 8. lastApplied的判断是用作同步appendEntry的时候，apply()function更新logEntry的同步问题
	// 特别是最后一次循环的时候，因为发送channel会解锁，不一定会调度到condInstallSnapshot中，有可能
	// 会整好先调度到下面的rf.snaping = false，再调度到appendEntry的处理函数，之前的各种问题就是因为这个
	// channel的解锁后，调度到不同goroutine的问题，现在的结果是一个综合处理
	if rf.logEntries.len() == rf.logEntries.getIndex0() || rf.logEntries.getIndex0() <= rf.lastApplied {
		return
	}
	rf.snaping = true
	DPrintf("%d recv snapshot[%d-%d]\n", rf.me, rf.logEntries.getIndex0(), rf.logEntries.lastIndex())
	// 9. 循环同步
	for _, log := range rf.logEntries.getEntriesOnlyread() {
		if log.Index < rf.logEntries.getIndex0() || log.Index < rf.lastApplied {
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
		DPrintf("%d applysnapshot1 %v apply: %v\n", rf.me, log.Command, applyMsg)
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
		DPrintf("%d applysnapshot2 %v apply: %v\n", rf.me, log.Command, applyMsg)
	}
	// 10. 更新follower commitIndex
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = min(rf.logEntries.len(), args.LastIncludedIndex)
		DPrintf("%d have updated commitIndex into %d\n", rf.me, rf.commitIndex)
	}
	// 11. 解逻辑锁
	rf.snaping = false
	// rf.commitIndex = args.LastIncludedIndex
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
