package raft

import (
	"bytes"

	"6.824/labgob"
)

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

	rf.logEntries.modify(tmpLog[rf.logEntries.Index0:])
	if rf.logEntries.len() == rf.logEntries.getIndex0() || rf.logEntries.getIndex0() <= rf.lastApplied {
		return
	}
	rf.snaping = true
	DPrintf("%d recv snapshot[%d-%d]\n", rf.me, rf.logEntries.getIndex0(), rf.logEntries.lastIndex())
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
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = min(rf.logEntries.len(), args.LastIncludedIndex)
		DPrintf("%d have updated commitIndex into %d\n", rf.me, rf.commitIndex)
	}
	rf.snaping = false
	// rf.commitIndex = args.LastIncludedIndex
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Installsnapshot", args, reply)
	return ok
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
	DPrintf("%d snapshot[%d]!!!!%d\n", rf.me, logIndex, peer)
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
		DPrintf("snapshot %d append nextIndex[%d]:%v\n", rf.me, peer, rf.nextIndex)
	} else {
		rf.mu.Lock()
	}

}
