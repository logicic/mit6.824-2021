package kvraft

import (
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[Server] <Get> %d recv %v\n", kv.me, args)
	command := Op{
		Op:        0,
		Key:       args.Key,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <Get> follower[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Get> LOCK[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	kv.mu.Lock()
	DPrintf("[Server] <Get> leader[%d] begin! At:%v\n", kv.me, args)
	clientSet, ok := kv.clientCh[args.ClientID]
	if !ok {
		kv.clientCh[args.ClientID] = make(map[int64]chan int64)
		clientSet = kv.clientCh[args.ClientID]
	}
	lcID, ok := kv.lastCommandID[args.ClientID]
	if !ok {
		lcID = -1
		kv.lastCommandID[args.ClientID] = -1
	}
	if lcID > args.CommandID {
		reply.Err = OK
		value, ok := kv.kvStore[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = value
		kv.mu.Unlock()

		return
	}
	commandCh, ok := clientSet[args.CommandID]
	if !ok {
		kv.clientCh[args.ClientID][args.CommandID] = make(chan int64)
		commandCh = kv.clientCh[args.ClientID][args.CommandID]
	}
	DPrintf("[Server] <Get> UNLOCK[%d]!", kv.me)
	kv.mu.Unlock()
	DPrintf("[Server] <Get> WAIT commandCH[%d]!", kv.me)
	reply.Err = OK
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			return
		}
		kv.mu.Lock()
		// delete last CommandID not this one
		delete(kv.clientCh[args.ClientID], command.CommandID)
		delete(kv.clientCh[args.ClientID], command.CommandID-1)
		value, ok := kv.kvStore[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = value
		kv.mu.Unlock()
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}

	// reply.Value = value
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
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <PutAppend> follower[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
		return
	}
	command.Term = term
	DPrintf("[Server] <PutAppend> LOCK[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	kv.mu.Lock()
	now := time.Now()
	DPrintf("[Server] <PutAppend> leader[%d] begin! At:%v\n", kv.me, args)
	clientSet, ok := kv.clientCh[args.ClientID]
	if !ok {
		DPrintf("[Server] <PutAppend> leader[%d] register client[%d]!\n", kv.me, args.ClientID)
		kv.clientCh[args.ClientID] = make(map[int64]chan int64)
		clientSet = kv.clientCh[args.ClientID]
	}
	lcID, ok := kv.lastCommandID[args.ClientID]
	if !ok {
		lcID = -1
		kv.lastCommandID[args.ClientID] = -1
	}
	if lcID > args.CommandID {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	commandCh, ok := clientSet[args.CommandID]
	if !ok {
		DPrintf("[Server] <PutAppend> term[%d] leader[%d] register client[%d] command[%d]!\n", command.Term, kv.me, args.ClientID, args.CommandID)
		kv.clientCh[args.ClientID][args.CommandID] = make(chan int64)
		commandCh = kv.clientCh[args.ClientID][args.CommandID]
	}
	DPrintf("[Server] <PutAppend> UNLOCK[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	kv.mu.Unlock()
	DPrintf("[Server] <PutAppend> WAIT commandCH[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	reply.Err = OK
	select {
	case applyCom := <-commandCh:
		if applyCom != command.CommandID {
			return
		}
		kv.mu.Lock()
		delete(kv.clientCh[args.ClientID], command.CommandID)
		delete(kv.clientCh[args.ClientID], command.CommandID-1)
		kv.mu.Unlock()
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	// if kv.clientCh[args.ClientID][command.CommandID-1] != nil {
	// 	close(kv.clientCh[args.ClientID][command.CommandID-1])
	// 	delete(kv.clientCh[args.ClientID], command.CommandID-1)
	// }
	DPrintf("[Server] <PutAppend> %d finish! At:%v ClientID[%d] ComandID[%d]\n", kv.me, time.Since(now), args.ClientID, args.CommandID)
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case c := <-kv.applyCh:
			DPrintf("%d applyCh:%v\n", kv.me, c)
			op := c.Command.(Op)
			kv.mu.Lock()
			lcID, ok := kv.lastCommandID[op.ClientID]
			if !ok {
				lcID = -1
				kv.lastCommandID[op.ClientID] = -1
			}
			// if lcID >= op.CommandID {
			// 	DPrintf("[Server] <applier> %d duplicate op[%d]:%v\n", kv.me, kv.lastCommandID, op)
			// 	kv.mu.Unlock()
			// 	continue
			// }
			if lcID < op.CommandID {
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
			cs, ok := kv.clientCh[op.ClientID]
			if !ok {
				DPrintf("[Server] <applier> %d no clientCH clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
				kv.mu.Unlock()
				continue
			}
			commandCh, ok := cs[op.CommandID]
			if !ok {
				DPrintf("[Server] <applier> %d no commandCH clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()
			// select {
			// case <-time.After(1 * time.Second):
			// 	close(commandCh)
			// 	delete(kv.clientCh[op.ClientID], op.CommandID)
			// 	DPrintf("[Server] <applier> %d out-date command clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
			// case commandCh <- op.CommandID:
			// }
			DPrintf("[Server] <applier> %d enter rf.GetState clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
			if term, isLeader := kv.rf.GetState(); isLeader && op.Term >= term {
				DPrintf("[Server] <applier> %d sendback1 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
				commandCh <- op.CommandID
				DPrintf("[Server] <applier> %d sendback2 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
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
	// You may need initialization code here.
	kv.clientCh = make(map[int64]map[int64]chan int64)
	kv.lastCommandID = make(map[int64]int64)
	go kv.applier()
	return kv
}
