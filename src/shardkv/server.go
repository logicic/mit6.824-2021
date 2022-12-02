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
	CommandType int
	ShardTask   int
	Config      shardctrler.Config
	ShardTasks  map[int]struct{}
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
	Config        shardctrler.Config
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
	term, isLeader1 := kv.rf.GetState()
	if !isLeader1 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("[Server] <Get> gid: %d follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		return
	}
	// 0. check shard
	shardTask := key2shard(command.Key)
	if kv.config.Shards[shardTask] != kv.gid {
		DPrintf("[Server] <Get> ErrWrongGroup gid:%d  %d recv %v\n", kv.gid, kv.me, args)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	command.ShardTask = shardTask
	if kv.shardKvStore.status(shardTask) != ShardNormal || kv.status != ConfigNormal {
		DPrintf("[Server] <Get> ErrShardWaiting gid:%d  %d recv %v shard[%d]:%v\n", kv.gid, kv.me, kv.status, shardTask, kv.shardKvStore.status(shardTask))
		kv.mu.Unlock()
		reply.Err = ErrShardWaiting
		return
	}
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

	// 2. check leader role and append logEntry

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
		kv.shardKvStore.print(kv.gid, kv.me)
		kv.mu.Unlock()
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	DPrintf("[Server] <Get> gid:%d %d finish! ClientID[%d] ComandID[%d] Err:%s\n", kv.gid, kv.me, args.ClientID, args.CommandID, reply.Err)
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
	term, isLeader1 := kv.rf.GetState()
	if !isLeader1 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <PutAppend> gid:%d  follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	// 0. check shard
	shardTask := key2shard(command.Key)
	if kv.config.Shards[shardTask] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	command.ShardTask = shardTask
	if kv.shardKvStore.status(shardTask) != ShardNormal || kv.status != ConfigNormal {
		kv.mu.Unlock()
		reply.Err = ErrShardWaiting
		return
	}
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	// 2. check leader role and append logEntry

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
	DPrintf("[Server] <PutAppend> gid:%d %d finish! ClientID[%d] ComandID[%d] Err:%s\n", kv.gid, kv.me, args.ClientID, args.CommandID, reply.Err)
}

func (kv *ShardKV) updateKVWithoutLOCK(op Op) {
	if op.Op == 1 {
		// Put
		kv.shardKvStore.put(op.ShardTask, op.Key, op.Value)
		DPrintf("[Server] <applier> gid:%d %d op:[PUT] clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 2 {
		// Append
		kv.shardKvStore.append(op.ShardTask, op.Key, op.Value)
		DPrintf("[Server] <applier> gid:%d %d op:[APPEND] clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 0 {
		// GET
		DPrintf("[Server] <applier> gid:%d %d op:[GET] clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
	}
	kv.shardKvStore.print(kv.gid, kv.me)
	kv.lastCommandID[op.ClientID] = op.CommandID
}

func (kv *ShardKV) updateConfigWithoutLOCK(op Op) {
	if kv.config.Num < op.Config.Num {
		kv.config = op.Config

		// kv.shardTasks = make(map[int]struct{})
		// for k, v := range op.ShardTasks {
		// 	kv.shardTasks[k] = v
		// }
	}
	kv.status = ConfigNormal
	kv.shardKvStore.clearStatus()
	fmt.Printf("updateConfigWithoutLOCK gid: %d me:%d  kv.config: %d\n", kv.gid, kv.me, kv.config.Num)
	kv.lastCommandID[op.ClientID] = op.CommandID
}

func (kv *ShardKV) installShardWithoutLOCK(op Op) {
	kv.shardKvStore.install(op.ShardTask, op.DB)
	fmt.Printf("gid: %d me:%d UNlock shard: %d\n", kv.gid, kv.me, op.ShardTask)
	kv.lastCommandID[op.ClientID] = op.CommandID
	kv.shardKvStore.setStatus(op.ShardTask, ShardNormal)
	// if _, isLeader := kv.rf.GetState(); isLeader {
	// 	kv.shardKvStore.setStatus(op.ShardTask, ShardNormal)
	// 	DPrintf("gid: %d me:%d UNlock shard: %d\n", kv.gid, kv.me, op.ShardTask)
	// 	// 	if kv.shardKvStore.allStatus() {
	// 	// 		if kv.config.Num < op.Config.Num {
	// 	// 			kv.appendUpdateConfig(op.Config)
	// 	// 			DPrintf("gid: %d me:%d UNlock kv.config: %v\n", kv.gid, kv.me, kv.config)
	// 	// 		}
	// 	// 	}
	// }

	// fmt.Printf("group[%d] %d kv:%v\n", kv.gid, kv.me, kv.shardKvStore.shard(op.ShardTask))
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
			Config:        kv.config,
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
		kv.config = data.Config
		// fmt.Printf("gid:%d me:%d kv:%v\n", kv.gid, kv.me, kv.shardKvStore[0].KvStore)
	}
}

func (kv *ShardKV) doCommandWithoutLOCK(op Op) bool {
	switch op.CommandType {
	case ExecuteCommandType:
		if kv.status != ConfigNormal {
			fmt.Printf("!!!!!!!!!!gid:%d me:%d value:%v\n", kv.gid, kv.me, op.Value)
			return false
		}
		kv.updateKVWithoutLOCK(op)
	case InstallShardCommandType:
		kv.installShardWithoutLOCK(op)
	case DeleteShardCommandType:
	case UpdateConfigCommandType:
		kv.updateConfigWithoutLOCK(op)
	}
	return true
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
				fmt.Printf("gid:%d %d applyCh:%v\n", kv.gid, kv.me, m)
				op := m.Command.(Op)
				kv.mu.Lock()
				// 1. check commandID
				// 2. only the new one can update keyvalue db
				if kv.checkCommandIDWithoutLOCK(op) {
					// 2.1 update k-v
					if !kv.doCommandWithoutLOCK(op) {
						kv.mu.Unlock()
						continue
					}
					// 2.2 get command channel
					// 3. snapshot
					kv.snapshotWithoutLOCK(m.CommandIndex)
					commandCh := kv.getCommandChanWithoutLOCK(op)
					kv.mu.Unlock()
					if commandCh != nil {
						DPrintf("[Server] <applier> gid:%d %d enter rf.GetState clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
						// 2.3 only leader role can send data to channel
						if term, isLeader := kv.rf.GetState(); isLeader && op.Term >= term {
							DPrintf("[Server] <applier> gid: %d %d sendback1 clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
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

func (kv *ShardKV) appendUpdateConfig(config shardctrler.Config) {
	command := Op{
		CommandType: UpdateConfigCommandType,
		ClientID:    int64(kv.gid),
		CommandID:   kv.lastCommandID[int64(kv.gid)] + 1,
		Config:      config,
		ShardTasks:  make(map[int]struct{}),
	}
	// for k, v := range kv.shardTasks {
	// 	command.ShardTasks[k] = v
	// }
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
func (kv *ShardKV) shardTaskMoved(curConfig, newConfig shardctrler.Config) (addShardTasks []int, redShardTasks []int) {

	shardTasks := make(map[int]struct{})
	for shardID, gid := range curConfig.Shards {
		if gid == kv.gid {
			// find out the new shardID
			if _, ok := shardTasks[shardID]; !ok {
				shardTasks[shardID] = struct{}{}
			}
		}
	}

	for shardID, gid := range newConfig.Shards {
		if gid == kv.gid {
			// find out the new shardID
			if _, ok := shardTasks[shardID]; !ok {
				addShardTasks = append(addShardTasks, shardID)
			}
		}
	}

	for shardId := range shardTasks {
		if newConfig.Shards[shardId] != kv.gid {
			// find out the moved shardID
			redShardTasks = append(redShardTasks, shardId)
		}
	}
	return
}

func (kv *ShardKV) configStatusMachine() {
	curConfig := kv.config
	newConfig := kv.sm.Query(curConfig.Num + 1)
	if newConfig.Num <= curConfig.Num {
		return
	}

	fmt.Printf("checkConfig %d gid:%d kv:%d newconfig:%v\n", kv.status, kv.gid, kv.me, newConfig)
	fmt.Printf("checkConfig %d gid:%d kv:%d oldconfig:%v\n", kv.status, kv.gid, kv.me, curConfig)
	// normal
	for kv.status == ConfigNormal {
		kv.shardKvStore.clearStatus()
		kv.status = ConfigRunning
		if curConfig.Num == 0 {
			kv.status = ConfigUpdate
			break
		}

		addShardTasks, redShardTasks := kv.shardTaskMoved(curConfig, newConfig)
		fmt.Printf("%d %d addShardTasks:%v redShardTasks:%v\n", kv.gid, kv.me, addShardTasks, redShardTasks)
		if len(addShardTasks) == 0 && len(redShardTasks) == 0 {
			// only update config
			kv.status = ConfigUpdate
			break
		}

		for _, shard := range addShardTasks {
			kv.shardKvStore.setStatus(shard, ShardWaiting)
			DPrintf("gid: %d me:%d lock shard: %d\n", kv.gid, kv.me, shard)
		}

		// 3.2 reduce shard task and send the shard db to new gid
		for _, shard := range redShardTasks {
			kv.shardKvStore.setStatus(shard, ShardSending)
			go kv.SendShard(shard, newConfig)
		}
		break
	}

	// running
	if kv.status == ConfigRunning {
		if kv.shardKvStore.isNormal() {
			fmt.Printf("gid: %d me:%d chenaggong! %v\n", kv.gid, kv.me, newConfig)
			kv.status = ConfigUpdate
		}

	}

	// updating
	if kv.status == ConfigUpdate {
		kv.status = ConfigUpdating
		kv.appendUpdateConfig(newConfig)
	}

	if kv.status == ConfigUpdating {
		return
	}
}

/*
检查configuration的标准：
0. 由于shardController对于group的join和leave会对shard-gid进行reblance
1. 需要对比当前的server所属的gid，以及match的shard是否发生了变化（与上一次检查所保留的结果）
2. 如果发生了变化，放弃此次所有的client request，把当前的已经commited的kv以及（commited和未commited）log转发
到真正shard的server上（需要只是leader发送？）
*/
// func (kv *ShardKV) checkConfig() {
// 	curConfig := kv.config
// 	newConfig := kv.sm.Query(curConfig.Num + 1)
// 	fmt.Printf("checkConfig gid:%d kv:%d newconfig:%v\n", kv.gid, kv.me, newConfig)
// 	fmt.Printf("checkConfig gid:%d kv:%d oldconfig:%v\n", kv.gid, kv.me, curConfig)
// 	// 1. check whether config is new
// 	if newConfig.Num <= curConfig.Num {
// 		return
// 	}

// 	if kv.status == ConfigWaiting {
// 		return
// 	}
// 	// 2. check whether shard is modified
// 	addShardTasks, redShardTasks := kv.shardTaskMoved(curConfig, newConfig)
// 	fmt.Printf("%d addShardTasks:%v redShardTasks:%v\n", kv.me, addShardTasks, redShardTasks)
// 	// 2.2 changed
// 	// kv.config = newConfig
// 	if len(addShardTasks) == 0 && len(redShardTasks) == 0 {
// 		// only update config
// 		kv.status = ConfigWaiting
// 		kv.appendUpdateConfig(newConfig)
// 		return
// 	}

// 	// 3. migrate kv data and logEntry data

// 	// 3.3 update the new config
// 	// 3.1 add shard task and wait new shard db
// 	if curConfig.Num == 0 {
// 		kv.status = ConfigWaiting
// 		kv.appendUpdateConfig(newConfig)
// 		return
// 	}
// 	for _, shard := range addShardTasks {
// 		kv.shardKvStore[shard].Status = GroupWaiting
// 		DPrintf("gid: %d me:%d lock shard: %d\n", kv.gid, kv.me, shard)
// 	}

// 	// 3.2 reduce shard task and send the shard db to new gid
// 	for _, shard := range redShardTasks {
// 		// kv.shardKvStore.setStatus(shard, ConfigWaiting)
// 		kv.SendShard(shard, newConfig)
// 	}
// 	if len(redShardTasks) > 0 && len(addShardTasks) == 0 {
// 		kv.status = ConfigWaiting
// 		kv.appendUpdateConfig(newConfig)
// 	} else {
// 		kv.status = ConfigWaiting
// 	}
// 	// kv.appendUpdateConfig(newConfig)
// }

func (kv *ShardKV) checkShardTaskWithoutLOCK(key string) bool {
	shardTask := key2shard(key)
	return kv.config.Shards[shardTask] == kv.gid
}

func (kv *ShardKV) queryConfigWithoutLOCK() {
	kv.config = kv.sm.Query(-1)
	// for shardID, gid := range kv.config.Shards {
	// 	if gid == kv.gid {
	// 		kv.shardTasks[shardID] = struct{}{}
	// 	}
	// }
}

// 每隔一段时间取查看configuration
func (kv *ShardKV) ticker() {
	// only leader can do?
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// kv.checkConfig()
			kv.configStatusMachine()
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
	kv.status = ConfigNormal
	kv.shardTasks = make(map[int]struct{})
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.queryConfigWithoutLOCK()
	kv.shardKvStore = newShardKvStore(10)
	kv.clientCh = make(map[int64]map[int64]chan int64)
	kv.lastCommandID = make(map[int64]int64)
	snapshot := kv.rf.ReadSnapshot()
	kv.readsnapshotWithoutLOCK(snapshot)
	kv.shardKvStore.clearStatus()
	go kv.ticker()
	go kv.applier()
	return kv
}
