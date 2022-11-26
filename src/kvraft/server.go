package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        int
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
	Term      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore map[string]string
	// clientID 一对多 commandID， commandID 一对一 channel
	clientCh      map[int64]map[int64]chan int64
	lastCommandID map[int64]int64
}

type snapshotData struct {
	KvStore       map[string]string
	LastCommandID map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[Server] <Get> %d recv %v\n", kv.me, args)
	command := Op{
		Op:        0,
		Key:       args.Key,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	kv.mu.Lock()
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		reply.Err = OK
		value, ok := kv.kvStore[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = value
		kv.mu.Unlock()

		return
	}
	// 2. check leader role and append logEntry
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("[Server] <Get> follower[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Get> begin![%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <Get> UNLOCK[%d]!", kv.me)
	kv.mu.Unlock()
	DPrintf("[Server] <Get> WAIT commandCH[%d]!", kv.me)
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			return
		}
		kv.mu.Lock()
		kv.deleteCommandChanWithoutLOCK(command)
		value, ok := kv.kvStore[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = value
		DPrintf("%d kv:%v\n", kv.me, kv.kvStore)
		kv.mu.Unlock()
		return
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
		kv.mu.Lock()
		kv.deleteCommandChanWithoutLOCK(command)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[Server] <PutAppend> %d recv %v\n", kv.me, args)
	o := -1
	if args.Op == "Put" {
		o = 1
	} else if args.Op == "Append" {
		o = 2
	} else {
		return
	}
	command := Op{
		Op:        o,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	kv.mu.Lock()
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// 2. check leader role and append logEntry
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <PutAppend> follower[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	DPrintf("[Server] <PutAppend> begin![%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <PutAppend> UNLOCK[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	kv.mu.Unlock()
	DPrintf("[Server] <PutAppend> WAIT commandCH[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	reply.Err = OK
	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != command.CommandID {
			return
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	kv.mu.Lock()
	kv.deleteCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	DPrintf("[Server] <PutAppend> %d finish! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
}

func (kv *KVServer) updateKVWithoutLOCK(op Op) {
	if op.Op == 1 {
		// Put
		kv.kvStore[op.Key] = op.Value
		DPrintf("[Server] <applier> %d op:[PUT] clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 2 {
		// Append
		kv.kvStore[op.Key] = kv.kvStore[op.Key] + op.Value
		DPrintf("[Server] <applier> %d op:[APPEND] clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 0 {
		// GET
		DPrintf("[Server] <applier> %d op:[GET] clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
	}
	kv.lastCommandID[op.ClientID] = op.CommandID
}

func (kv *KVServer) makeCommandChanWithoutLOCK(op Op) chan int64 {
	clientSet, ok := kv.clientCh[op.ClientID]
	if !ok {
		kv.clientCh[op.ClientID] = make(map[int64]chan int64)
		clientSet = kv.clientCh[op.ClientID]
	}
	commandCh, ok := clientSet[op.CommandID]
	if !ok {
		kv.clientCh[op.ClientID][op.CommandID] = make(chan int64)
		commandCh = kv.clientCh[op.ClientID][op.CommandID]
	}
	return commandCh
}

func (kv *KVServer) getCommandChanWithoutLOCK(op Op) chan int64 {
	clientSet, ok := kv.clientCh[op.ClientID]
	if !ok {
		return nil
	}
	commandCh, ok := clientSet[op.CommandID]
	if !ok {
		return nil
	}
	return commandCh
}

func (kv *KVServer) deleteCommandChanWithoutLOCK(op Op) {
	if kv.clientCh[op.ClientID][op.CommandID] != nil {
		close(kv.clientCh[op.ClientID][op.CommandID])
		delete(kv.clientCh[op.ClientID], op.CommandID)
	}
}

func (kv *KVServer) checkCommandIDWithoutLOCK(op Op) bool {
	lcID, ok := kv.lastCommandID[op.ClientID]
	if !ok {
		lcID = -1
		kv.lastCommandID[op.ClientID] = -1
	}
	if lcID >= op.CommandID {
		return false
	}
	return true
}

func (kv *KVServer) snapshotWithoutLOCK(commandIndex int) {
	if kv.rf.RaftStateSize() > kv.maxraftstate && kv.maxraftstate > -1 {
		data := snapshotData{
			KvStore:       kv.kvStore,
			LastCommandID: kv.lastCommandID,
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(data)
		kv.rf.Snapshot(commandIndex, w.Bytes())
	}
}
func (kv *KVServer) readsnapshotWithoutLOCK(snapshot []byte) {
	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var data snapshotData
		if d.Decode(&data) != nil {
			log.Fatalf("decode error\n")
		}
		kv.kvStore = data.KvStore
		kv.lastCommandID = data.LastCommandID
	}
}
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case m := <-kv.applyCh:
			if m.SnapshotValid {

				if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
					m.SnapshotIndex, m.Snapshot) {
					kv.mu.Lock()
					kv.readsnapshotWithoutLOCK(m.Snapshot)
					kv.mu.Unlock()
				}

			} else if m.CommandValid {
				DPrintf("%d applyCh:%v\n", kv.me, m)
				op := m.Command.(Op)
				kv.mu.Lock()
				// 1. check commandID
				// 2. only the new one can update keyvalue db
				if kv.checkCommandIDWithoutLOCK(op) {
					// 2.1 update k-v
					kv.updateKVWithoutLOCK(op)
					// 2.2 get command channel
					// 3. snapshot
					kv.snapshotWithoutLOCK(m.CommandIndex)
					commandCh := kv.getCommandChanWithoutLOCK(op)
					kv.mu.Unlock()
					if commandCh != nil {
						DPrintf("[Server] <applier> %d enter rf.GetState clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
						// 2.3 only leader role can send data to channel
						if term, isLeader := kv.rf.GetState(); isLeader && op.Term >= term {
							DPrintf("[Server] <applier> %d sendback1 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
							commandCh <- op.CommandID
							DPrintf("[Server] <applier> %d sendback2 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
						}
					}
				} else {
					kv.mu.Unlock()
				}
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.clientCh = make(map[int64]map[int64]chan int64)
	kv.lastCommandID = make(map[int64]int64)
	snapshot := kv.rf.ReadSnapshot()
	kv.readsnapshotWithoutLOCK(snapshot)
	go kv.applier()
	return kv
}
