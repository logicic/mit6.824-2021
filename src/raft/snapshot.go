package raft

import (
	"bytes"

	"6.824/labgob"
)

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
	DPrintf("%d LastIncludedIndex:%d commitIndex:%d Index0:%d lastApplied:%d\n", rf.me, args.LastIncludedIndex, rf.commitIndex, rf.logEntries.Index0, rf.lastApplied)
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
	DPrintf("%d recv snapshot[%d-%d]\n", rf.me, rf.logEntries.Entries[0].Index, rf.logEntries.Entries[len(rf.logEntries.Entries)-1].Index)
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
