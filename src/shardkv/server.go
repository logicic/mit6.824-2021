package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
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
	CommandType int
	ShardTask   int
	Config      shardctrler.Config
	DB          shardKvDB
	Op          int
	Key         string
	Value       string
	ClientID    int64
	CommandID   int64
	Term        int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm            *shardctrler.Clerk
	config        shardctrler.Config
	shardTasks    map[int]struct{}
	shardKvStore  shardKvStore
	clientCh      map[int64]map[int64]chan int64
	lastCommandID map[int64]int64
	status        int
}

type snapshotData struct {
	KvStore       shardKvStore
	LastCommandID map[int64]int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[Server] <Get> gid:%d  %d recv %v\n", kv.gid, kv.me, args)
	command := Op{
		CommandType: ExecuteCommandType,
		Op:          0,
		Key:         args.Key,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
	}
	kv.mu.Lock()
	// 0. check shard
	shardTask := key2shard(command.Key)
	if kv.config.Shards[shardTask] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	command.ShardTask = shardTask
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		reply.Err = OK
		value, ok := kv.shardKvStore.get(shardTask, command.Key)
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = value
		kv.mu.Unlock()

		return
	}

	if kv.status == GroupWaiting {
		kv.mu.Unlock()
		reply.Err = ErrShardWaiting
		return
	}

	// 2. check leader role and append logEntry
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("[Server] <Get> gid: %d follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Get> gid:%d begin![%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	DPrintf("[Server] <Get> gid:%d WAIT commandCH[%d]!", kv.gid, kv.me)
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			// return
			DPrintf("%d applyCom:%v commandID:%d\n", kv.me, applyCom, args.CommandID)
		}
		kv.mu.Lock()
		// kv.deleteCommandChanWithoutLOCK(command)
		value, ok := kv.shardKvStore.get(shardTask, command.Key)
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = value
		kv.mu.Unlock()
		return
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[Server] <PutAppend> gid:%d %d recv %v\n", kv.gid, kv.me, args)
	o := -1
	if args.Op == "Put" {
		o = 1
	} else if args.Op == "Append" {
		o = 2
	} else {
		return
	}
	command := Op{
		CommandType: ExecuteCommandType,
		Op:          o,
		Key:         args.Key,
		Value:       args.Value,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
	}
	kv.mu.Lock()
	// 0. check shard
	shardTask := key2shard(command.Key)
	if kv.config.Shards[shardTask] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	command.ShardTask = shardTask
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	if kv.status == GroupWaiting {
		kv.mu.Unlock()
		reply.Err = ErrShardWaiting
		return
	}

	// 2. check leader role and append logEntry
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <PutAppend> gid:%d  follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	DPrintf("[Server] <PutAppend> gid:%d begin![%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	DPrintf("[Server] <PutAppend> gid:%d WAIT commandCH[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
	reply.Err = OK
	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != command.CommandID {
			// return
			DPrintf("%d applyCom:%v commandID:%d\n", kv.me, applyCom, args.CommandID)
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	// kv.mu.Lock()
	// kv.deleteCommandChanWithoutLOCK(command)
	// kv.mu.Unlock()
	DPrintf("[Server] <PutAppend> gid:%d %d finish! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
}

func (kv *ShardKV) updateKVWithoutLOCK(op Op) {
	if op.Op == 1 {
		// Put
		kv.shardKvStore.put(op.ShardTask, op.Key, op.Value)
		DPrintf("[Server] <applier> %d op:[PUT] clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 2 {
		// Append
		kv.shardKvStore.append(op.ShardTask, op.Key, op.Value)
		DPrintf("[Server] <applier> %d op:[APPEND] clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 0 {
		// GET
		DPrintf("[Server] <applier> %d op:[GET] clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
	}
	kv.shardKvStore.print(kv.gid, key2shard(op.Key))
	kv.lastCommandID[op.ClientID] = op.CommandID
}

func (kv *ShardKV) updateConfigWithoutLOCK(op Op) {
	kv.config = op.Config
	kv.lastCommandID[op.ClientID] = op.CommandID
}

func (kv *ShardKV) installShardWithoutLOCK(op Op) {
	kv.shardKvStore.install(op.ShardTask, op.DB)
	kv.status = GroupServing
	kv.lastCommandID[op.ClientID] = op.CommandID
	fmt.Printf("group[%d] %d kv:%v\n", kv.gid, kv.me, kv.shardKvStore.shard(op.ShardTask))
}

func (kv *ShardKV) makeCommandChanWithoutLOCK(op Op) chan int64 {
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

func (kv *ShardKV) getCommandChanWithoutLOCK(op Op) chan int64 {
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

func (kv *ShardKV) deleteCommandChanWithoutLOCK(op Op) {
	if kv.clientCh[op.ClientID][op.CommandID] != nil {
		close(kv.clientCh[op.ClientID][op.CommandID])
		delete(kv.clientCh[op.ClientID], op.CommandID)
	}
}

func (kv *ShardKV) checkCommandIDWithoutLOCK(op Op) bool {
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

func (kv *ShardKV) snapshotWithoutLOCK(commandIndex int) {
	if kv.rf.RaftStateSize() > kv.maxraftstate && kv.maxraftstate > -1 {
		data := snapshotData{
			KvStore:       kv.shardKvStore,
			LastCommandID: kv.lastCommandID,
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(data)
		kv.rf.Snapshot(commandIndex, w.Bytes())
	}
}
func (kv *ShardKV) readsnapshotWithoutLOCK(snapshot []byte) {
	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var data snapshotData
		if d.Decode(&data) != nil {
			log.Fatalf("decode error\n")
		}
		kv.shardKvStore = data.KvStore
		kv.lastCommandID = data.LastCommandID
		fmt.Printf("gid:%d me:%d kv:%v\n", kv.gid, kv.me, kv.shardKvStore[0].KvStore)
	}
}

func (kv *ShardKV) doCommandWithoutLOCK(op Op) {
	switch op.CommandType {
	case ExecuteCommandType:
		kv.updateKVWithoutLOCK(op)
	case InstallShardCommandType:
		kv.installShardWithoutLOCK(op)
	case DeleteShardCommandType:
	case UpdateConfigCommandType:
		kv.updateConfigWithoutLOCK(op)
	}
}

func (kv *ShardKV) applier() {
	for {
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
					kv.doCommandWithoutLOCK(op)
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
							// commandCh <- op.CommandID
							select {
							case commandCh <- op.CommandID:
							case <-time.After(1 * time.Second):
							}
							DPrintf("[Server] <applier> %d sendback2 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
						}
						kv.mu.Lock()
						kv.deleteCommandChanWithoutLOCK(op)
						kv.mu.Unlock()
					}
				} else {
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *ShardKV) appendUpdateConfig() {
	command := Op{
		CommandType: UpdateConfigCommandType,
		ClientID:    int64(kv.gid),
		CommandID:   kv.lastCommandID[int64(kv.gid)] + 1,
		Config:      kv.config,
	}
	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		return
	}
}

/*
shard 任务变更情况，需要知道：
1. 这一个gid需要处理的shardNum变化情况
2. 变少了，需要发送相应shardNum的db给对应shardNum的新gid下的server
3. 变多了，需要等待对方的shardNum的db发过来，更新db再对外提供服务
4. （需要区分由gid0变1的特殊情况）
*/
func (kv *ShardKV) shardTaskMoved(newConfig shardctrler.Config) (addShardTasks []int, redShardTasks []int) {

	for shardID, gid := range newConfig.Shards {
		if gid == kv.gid {
			// find out the new shardID
			if _, ok := kv.shardTasks[shardID]; !ok {
				addShardTasks = append(addShardTasks, shardID)
				kv.shardTasks[shardID] = struct{}{}
			}
		}
	}

	for shardId := range kv.shardTasks {
		if newConfig.Shards[shardId] != kv.gid {
			// find out the moved shardID
			redShardTasks = append(redShardTasks, shardId)
			delete(kv.shardTasks, shardId)
		}
	}
	return
}

/*
检查configuration的标准：
0. 由于shardController对于group的join和leave会对shard-gid进行reblance
1. 需要对比当前的server所属的gid，以及match的shard是否发生了变化（与上一次检查所保留的结果）
2. 如果发生了变化，放弃此次所有的client request，把当前的已经commited的kv以及（commited和未commited）log转发
到真正shard的server上（需要只是leader发送？）
*/
func (kv *ShardKV) checkConfig() {
	newConfig := kv.sm.Query(-1)
	curCofnig := kv.config
	// 1. check whether config is new
	if newConfig.Num <= curCofnig.Num {
		return
	}

	// 2. check whether shard is modified
	addShardTasks, redShardTasks := kv.shardTaskMoved(newConfig)
	// 2.2 changed
	kv.config = newConfig
	if len(addShardTasks) == 0 && len(redShardTasks) == 0 {
		// only update config
		kv.appendUpdateConfig()
		return
	}

	// 3. migrate kv data and logEntry data

	// 3.3 update the new config
	kv.appendUpdateConfig()
	// 3.1 add shard task and wait new shard db
	if curCofnig.Num == 0 {
		return
	}
	if len(addShardTasks) > 0 {
		kv.status = GroupWaiting
	}
	fmt.Printf("%d addShardTasks:%v redShardTasks:%v\n", kv.me, addShardTasks, redShardTasks)
	// 3.2 reduce shard task and send the shard db to new gid
	for _, shard := range redShardTasks {
		kv.SendShard(shard)
	}

}

func (kv *ShardKV) checkShardTaskWithoutLOCK(key string) bool {
	shardTask := key2shard(key)
	return kv.config.Shards[shardTask] == kv.gid
}

func (kv *ShardKV) queryConfigWithoutLOCK() {
	kv.config = kv.sm.Query(-1)
	for shardID, gid := range kv.config.Shards {
		if gid == kv.gid {
			kv.shardTasks[shardID] = struct{}{}
		}
	}
}

// 每隔一段时间取查看configuration
func (kv *ShardKV) ticker() {
	// only leader can do?
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			kv.checkConfig()
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Microsecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.shardTasks = make(map[int]struct{})
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.queryConfigWithoutLOCK()
	kv.shardKvStore = newShardKvStore(10)
	kv.clientCh = make(map[int64]map[int64]chan int64)
	kv.lastCommandID = make(map[int64]int64)
	snapshot := kv.rf.ReadSnapshot()
	kv.readsnapshotWithoutLOCK(snapshot)
	go kv.ticker()
	go kv.applier()
	return kv
}
