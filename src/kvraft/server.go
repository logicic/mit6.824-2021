package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

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
	Op    int
	Key   string
	Value string
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
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%d GET kvStore:%v\n", kv.me, kv.kvStore)
	value, ok := kv.kvStore[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = value
	reply.Err = OK
	return
	// command := Op{
	// 	Op:  0,
	// 	Key: args.Key,
	// }
	// _, _, isLeader := kv.rf.Start(command)
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%d recv %v\n", kv.me, args)
	o := -1
	if args.Op == "Put" {
		o = 1
	} else if args.Op == "Append" {
		o = 2
	} else {
		return
	}
	command := Op{
		Op:    o,
		Key:   args.Key,
		Value: args.Value,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case c := <-kv.applyCh:
		op := c.Command.(Op)
		kv.mu.Lock()
		if op.Op == 1 {
			// Put
			kv.kvStore[op.Key] = op.Value
		} else if op.Op == 2 {
			// Append
			kv.kvStore[op.Key] = kv.kvStore[op.Key] + op.Value
		}
		DPrintf("%d kvStore:%v\n", kv.me, kv.kvStore)
		kv.mu.Unlock()
	}
	reply.Err = OK
	DPrintf("%d finish!\n", kv.me)
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {
		case c := <-kv.applyCh:
			op := c.Command.(Op)
			kv.mu.Lock()
			if op.Op == 1 {
				// Put
				kv.kvStore[op.Key] = op.Value
			} else if op.Op == 2 {
				// Append
				kv.kvStore[op.Key] = kv.kvStore[op.Key] + op.Value
			}
			kv.mu.Unlock()
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

	return kv
}
