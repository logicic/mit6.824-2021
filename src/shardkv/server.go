package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
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
	dead            int32 // set by Kill()
	sm              *shardctrler.Clerk
	config          shardctrler.Config
	lastConfig      shardctrler.Config
	shardKvStore    shardKvStore
	clientCh        map[int64]map[int64]chan int64
	deletingShard   []int
	shardsMigrateWG *sync.WaitGroup
}

type snapshotData struct {
	KvStore       shardKvStore
	DeletingShard []int
	Config        shardctrler.Config
	LastConfig    shardctrler.Config
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
		return
	}
	if args.ConfigNum != kv.config.Num || args.ConfigNum == 0 {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
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
	if kv.shardKvStore.status(shardTask) != ShardNormal {
		kv.mu.Unlock()
		reply.Err = ErrShardWaiting
		return
	}
	// 1. check duplicate and out-date data
	if !kv.shardKvStore.checkCommandIDWithoutLOCK(command.ShardTask, command.ClientID, command.CommandID) {
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
	if !isLeader2 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom == Waiting {
			// return
			reply.Err = ErrSendAgain
			DPrintf("[Server] <Get> gid:%d %d finish! ClientID[%d] ComandID[%d] Err:%s\n", kv.gid, kv.me, args.ClientID, args.CommandID, reply.Err)
			return
		}
		kv.mu.Lock()
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
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum != kv.config.Num || args.ConfigNum == 0 {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
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
	if kv.shardKvStore.status(shardTask) != ShardNormal {
		kv.mu.Unlock()
		reply.Err = ErrShardWaiting
		return
	}
	// 1. check duplicate and out-date data
	if !kv.shardKvStore.checkCommandIDWithoutLOCK(command.ShardTask, command.ClientID, command.CommandID) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	// 2. check leader role and append logEntry

	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	reply.Err = OK
	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom == Waiting {
			reply.Err = ErrSendAgain
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	DPrintf("[Server] <PutAppend> gid:%d %d finish! ClientID[%d] ComandID[%d] Err:%s\n", kv.gid, kv.me, args.ClientID, args.CommandID, reply.Err)
}

func (kv *ShardKV) updateKVWithoutLOCK(op Op) {
	if op.Op == 1 {
		// Put
		kv.shardKvStore.put(op.ShardTask, op.Key, op.Value)
		DPrintf("[Server] <updateKVWithoutLOCK> gid:%d %d op:[PUT] clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 2 {
		// Append
		kv.shardKvStore.append(op.ShardTask, op.Key, op.Value)
		DPrintf("[Server] <updateKVWithoutLOCK> gid:%d %d op:[APPEND] clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
	} else if op.Op == 0 {
		// GET
		DPrintf("[Server] <updateKVWithoutLOCK> gid:%d %d op:[GET] clientID[%d] commandID[%d]\n", kv.gid, kv.me, op.ClientID, op.CommandID)
	}
	kv.shardKvStore.print(kv.gid, kv.me)
	kv.shardKvStore.setCommandIDWithoutLOCK(op.ShardTask, op.ClientID, op.CommandID)
}

func (kv *ShardKV) updateConfigWithoutLOCK(op Op) {
	DPrintf("[Server] <updateConfigWithoutLOCK> gid:%d %d current config:%d new config:%d\n", kv.gid, kv.me, kv.config.Num, op.Config.Num)
	if kv.config.Num < op.Config.Num {
		// update shard status
		kv.shardKvStore.clearStatus()
		if kv.config.Num > 0 {
			addShard, redShard := kv.shardTaskMoved(kv.config, op.Config)
			for _, shard := range addShard {
				kv.shardKvStore.setStatus(shard, ShardWaiting)
			}

			for _, shard := range redShard {
				kv.shardKvStore.setStatus(shard, ShardSending)
			}
			kv.deletingShard = redShard
		}
		kv.lastConfig = kv.config
		kv.config = op.Config
	}
}

func (kv *ShardKV) installShardWithoutLOCK(op Op) {
	if kv.config.Num <= op.Config.Num {
		kv.shardKvStore.install(op.ShardTask, op.DB)
	}
	kv.shardKvStore.setStatus(op.ShardTask, ShardNormal)
	DPrintf("[Server] <installShardWithoutLOCK> gid: %d me:%d Install shard: %d db:%v\n", kv.gid, kv.me, op.ShardTask, kv.shardKvStore.shard(op.ShardTask))
}

func (kv *ShardKV) deleteShardWithoutLOCK(op Op) {
	if kv.config.Num <= op.Config.Num {
		kv.shardKvStore.delete(op.ShardTask)
	}
	kv.shardKvStore.setStatus(op.ShardTask, ShardNormal)
	DPrintf("[Server] <deleteShardWithoutLOCK> gid: %d me:%d delete shard: %d db:%v\n", kv.gid, kv.me, op.ShardTask, kv.shardKvStore.shard(op.ShardTask))
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

func (kv *ShardKV) snapshotWithoutLOCK(commandIndex int) {
	if kv.rf.RaftStateSize() > kv.maxraftstate && kv.maxraftstate > -1 {
		data := snapshotData{
			KvStore:       newShardKvStore(10),
			DeletingShard: kv.deletingShard,
			Config:        kv.config,
			LastConfig:    kv.lastConfig,
		}
		data.KvStore.deepcopy(kv.shardKvStore)
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
		kv.shardKvStore.deepcopy(data.KvStore)
		kv.config = data.Config
		kv.lastConfig = data.LastConfig
		copy(kv.deletingShard, data.DeletingShard)
	}
}

func (kv *ShardKV) applier() {
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
				DPrintf("gid:%d %d applyCh:%v\n", kv.gid, kv.me, m)
				op := m.Command.(Op)
				kv.mu.Lock()
				// 1. check commandID
				// 2. only the new one can update keyvalue db
				ok := true
				switch op.CommandType {
				case ExecuteCommandType:
					if kv.shardKvStore.checkCommandIDWithoutLOCK(op.ShardTask, op.ClientID, op.CommandID) {
						if kv.shardKvStore.status(op.ShardTask) != ShardNormal {
							ok = false
							break
						}
						shardTask := key2shard(op.Key)
						if kv.config.Shards[shardTask] != kv.gid {
							ok = false
							break
						}

						kv.updateKVWithoutLOCK(op)
					} else {
						kv.mu.Unlock()
						continue
					}
				case InstallShardCommandType:
					kv.installShardWithoutLOCK(op)
				case DeleteShardCommandType:
					kv.deleteShardWithoutLOCK(op)
				case UpdateConfigCommandType:
					kv.updateConfigWithoutLOCK(op)
				}

				kv.snapshotWithoutLOCK(m.CommandIndex)
				commandCh := kv.getCommandChanWithoutLOCK(op)
				kv.mu.Unlock()
				if commandCh != nil {
					// 2.3 only leader role can send data to channel
					if term, isLeader := kv.rf.GetState(); isLeader && op.Term >= term {
						// commandCh <- op.CommandID
						sendCommandID := op.CommandID
						if !ok {
							sendCommandID = Waiting
						}
						select {
						case commandCh <- sendCommandID:
						case <-time.After(1 * time.Second):
						}
					}
					kv.mu.Lock()
					kv.deleteCommandChanWithoutLOCK(op)
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *ShardKV) appendUpdateConfig(config shardctrler.Config) {
	DPrintf("[Server] <appendUpdateConfig> gid: %d me:%d append newconfig: %v\n", kv.gid, kv.me, config)
	command := Op{
		CommandType: UpdateConfigCommandType,
		ClientID:    int64(kv.gid),
		CommandID:   nrand(),
		Config:      config,
	}

	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		kv.mu.Unlock()
		return
	}
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	select {
	case <-commandCh:
	case <-time.After(ExecuteTimeout):
	}
}

func (kv *ShardKV) appendDelete(config shardctrler.Config, shard int) {
	kv.mu.Lock()
	DPrintf("[Server] <appendDelete> gid: %d me:%d delete shard: %d when config:%d\n", kv.gid, kv.me, shard, config.Num)
	command := Op{
		CommandType: DeleteShardCommandType,
		ClientID:    kv.shardKvStore.id(shard),
		CommandID:   kv.shardKvStore.getCommandIDWithoutLOCK(shard, kv.shardKvStore.id(shard)) + 1,
		Config:      config,
		ShardTask:   shard,
	}

	term, isLeader1 := kv.rf.GetState()
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		return
	}
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	kv.mu.Unlock()
	select {
	case <-commandCh:
	case <-time.After(ExecuteTimeout):
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
	DPrintf("[Server] <shardTaskMoved> gid:%d %d current config:%d new config:%d addShard:%v redShard:%v\n", kv.gid, kv.me, curConfig.Num, newConfig.Num, addShardTasks, redShardTasks)
	return
}

func (kv *ShardKV) checkShardTaskWithoutLOCK(key string) bool {
	shardTask := key2shard(key)
	return kv.config.Shards[shardTask] == kv.gid
}

// 每隔一段时间取查看configuration
func (kv *ShardKV) ticker(run func()) {
	// only leader can do?
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			run()
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (kv *ShardKV) configMove() {
	kv.mu.Lock()
	curConfig := kv.config
	newConfig := kv.sm.Query(curConfig.Num + 1)
	if newConfig.Num <= curConfig.Num {
		kv.mu.Unlock()
		return
	}

	// check shardDB status
	kv.shardKvStore.print(kv.gid, kv.me)
	if kv.shardKvStore.isNormal() {
		DPrintf("[Server] <configMove> gid:%d %d update config[%d]\n", kv.gid, kv.me, kv.config.Num)
		kv.appendUpdateConfig(newConfig)
	} else {
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) shardMove() {
	kv.shardsMigrateWG.Wait()
	kv.mu.Lock()
	redShard := kv.shardKvStore.shardSlice(ShardSending)
	kv.mu.Unlock()
	DPrintf("[Server] <shardMove> me[%d] gid:%d config[%d] kv.deletingShard:%v\n", kv.me, kv.gid, kv.config.Num, redShard)
	for _, shard := range redShard {
		kv.shardsMigrateWG.Add(1)
		go func(shard int) {
			kv.mu.Lock()
			args := InstallShardArgs{
				ClientID:  kv.shardKvStore.id(shard),
				CommandID: kv.shardKvStore.getCommandIDWithoutLOCK(shard, kv.shardKvStore.id(shard)) + 1,
				DB:        kv.shardKvStore.shard(shard),
				ShardNum:  shard,
				Config:    kv.config,
			}
			kv.mu.Unlock()
			gid := args.Config.Shards[args.ShardNum]
			if servers, ok := args.Config.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					var reply InstallShardReply
					ok := srv.Call("ShardKV.InstallShard", &args, &reply)
					DPrintf("[Server] <shardMove> me[%d] sendergid:%d config[%d] recvgid:%d recvme:%d\n", kv.me, kv.gid, args.Config.Num, gid, si)
					if ok && reply.Err == OK {
						kv.appendDelete(args.Config, args.ShardNum)
						break
					}
				}
			}
			kv.shardsMigrateWG.Done()
		}(shard)
	}
	kv.shardsMigrateWG.Wait()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	DPrintf("gid: %d me: %d begin!\n", kv.gid, kv.me)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.deletingShard = make([]int, 0)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.sm.Query(0)
	kv.lastConfig = kv.sm.Query(0)
	kv.shardsMigrateWG = &sync.WaitGroup{}
	kv.shardKvStore = newShardKvStore(10)
	kv.clientCh = make(map[int64]map[int64]chan int64)
	snapshot := kv.rf.ReadSnapshot()
	kv.readsnapshotWithoutLOCK(snapshot)
	DPrintf("gid:%d me:%d configNum:%d addShard:%v redShard:%d\n", kv.gid, kv.me, kv.config.Num, kv.shardKvStore.shardSlice(ShardWaiting), kv.shardKvStore.shardSlice(ShardSending))
	go kv.ticker(kv.configMove)
	go kv.ticker(kv.shardMove)
	go kv.applier()
	return kv
}
