package shardctrler

import (
	"log"
	"math"
	"sort"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead          int32    // set by Kill()
	configs       []Config // indexed by config num
	clientCh      map[int64]map[int64]chan int64
	lastCommandID map[int64]int64
}

/*
0: query
1: join
2: leave
3: move
*/
type Op struct {
	// Your data here.
	Op        int
	Num       int
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	ClientID  int64
	CommandID int64
	Term      int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("[Server] <Join> %d recv %v\n", sc.me, args)
	sc.mu.Lock()
	command := Op{
		Op:        1,
		Servers:   args.Servers,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}

	// 1. check duplicate and out-date data
	if !sc.checkCommandIDWithoutLOCK(command) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 2. check leader role and append logEntry
	term, isLeader1 := sc.rf.GetState()
	command.Term = term
	_, term, isLeader2 := sc.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		DPrintf("[Server] <Join> follower[%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Join> begin![%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := sc.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <Join> UNLOCK[%d]!", sc.me)
	sc.mu.Unlock()
	DPrintf("[Server] <Join> WAIT commandCH[%d]!", sc.me)
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			// return
			DPrintf("%d applyCom:%v commandID:%d\n", sc.me, applyCom, args.CommandID)
		}
		return
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("[Server] <Leave> %d recv %v\n", sc.me, args)
	sc.mu.Lock()
	command := Op{
		Op:        2,
		GIDs:      args.GIDs,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}

	// 1. check duplicate and out-date data
	if !sc.checkCommandIDWithoutLOCK(command) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 2. check leader role and append logEntry
	term, isLeader1 := sc.rf.GetState()
	command.Term = term
	_, term, isLeader2 := sc.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		DPrintf("[Server] <Leave> follower[%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Leave> begin![%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := sc.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <Leave> UNLOCK[%d]!", sc.me)
	sc.mu.Unlock()
	DPrintf("[Server] <Leave> WAIT commandCH[%d]!", sc.me)
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			// return
			DPrintf("%d applyCom:%v commandID:%d\n", sc.me, applyCom, args.CommandID)
		}
		return
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("[Server] <Move> %d recv %v\n", sc.me, args)
	sc.mu.Lock()
	command := Op{
		Op:        3,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}

	// 1. check duplicate and out-date data
	if !sc.checkCommandIDWithoutLOCK(command) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 2. check leader role and append logEntry
	term, isLeader1 := sc.rf.GetState()
	command.Term = term
	_, term, isLeader2 := sc.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		DPrintf("[Server] <Move> follower[%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Move> begin![%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := sc.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <Move> UNLOCK[%d]!", sc.me)
	sc.mu.Unlock()
	DPrintf("[Server] <Move> WAIT commandCH[%d]!", sc.me)
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			// return
			DPrintf("%d applyCom:%v commandID:%d\n", sc.me, applyCom, args.CommandID)
		}
		return
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
}

func (sc *ShardCtrler) queryWithoutLOCK(num int) Config {
	if num < 0 {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("[Server] <Query> %d recv %v\n", sc.me, args)
	command := Op{
		Op:        0,
		Num:       args.Num,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	sc.mu.Lock()
	// 1. check duplicate and out-date data
	if !sc.checkCommandIDWithoutLOCK(command) {
		config := sc.queryWithoutLOCK(command.Num)
		reply.Config = config
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	// 2. check leader role and append logEntry
	term, isLeader1 := sc.rf.GetState()
	command.Term = term
	_, term, isLeader2 := sc.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		DPrintf("[Server] <Query> follower[%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
		return
	}

	DPrintf("[Server] <Query> begin![%d]! ClientID[%d] ComandID[%d]\n", sc.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := sc.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <Query> UNLOCK[%d]!", sc.me)
	sc.mu.Unlock()
	DPrintf("[Server] <Query> WAIT commandCH[%d]!", sc.me)
	reply.Err = OK

	// 4. wait the command data from channel
	select {
	case applyCom := <-commandCh:
		if applyCom != args.CommandID {
			// return
			DPrintf("%d applyCom:%v commandID:%d\n", sc.me, applyCom, args.CommandID)
		}
		sc.mu.Lock()
		config := sc.queryWithoutLOCK(command.Num)
		reply.Config = config
		reply.Err = OK
		sc.mu.Unlock()
		return
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) makeCommandChanWithoutLOCK(op Op) chan int64 {
	clientSet, ok := sc.clientCh[op.ClientID]
	if !ok {
		sc.clientCh[op.ClientID] = make(map[int64]chan int64)
		clientSet = sc.clientCh[op.ClientID]
	}
	commandCh, ok := clientSet[op.CommandID]
	if !ok {
		sc.clientCh[op.ClientID][op.CommandID] = make(chan int64)
		commandCh = sc.clientCh[op.ClientID][op.CommandID]
	}
	return commandCh
}

func (sc *ShardCtrler) getCommandChanWithoutLOCK(op Op) chan int64 {
	clientSet, ok := sc.clientCh[op.ClientID]
	if !ok {
		return nil
	}
	commandCh, ok := clientSet[op.CommandID]
	if !ok {
		return nil
	}
	return commandCh
}

func (sc *ShardCtrler) deleteCommandChanWithoutLOCK(op Op) {
	if sc.clientCh[op.ClientID][op.CommandID] != nil {
		close(sc.clientCh[op.ClientID][op.CommandID])
		delete(sc.clientCh[op.ClientID], op.CommandID)
	}
}

func (sc *ShardCtrler) checkCommandIDWithoutLOCK(op Op) bool {
	lcID, ok := sc.lastCommandID[op.ClientID]
	if !ok {
		lcID = -1
		sc.lastCommandID[op.ClientID] = -1
	}
	if lcID >= op.CommandID {
		return false
	}
	return true
}

func deepCopyMap(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func findMinMaxGid(gidShardCountMap map[int][]int) (int, int, int, int) {
	i := 0
	min := 0
	minGid := 0
	max := 0
	maxGid := 0
	for gid, shard := range gidShardCountMap {
		if i == 0 {
			min = len(shard)
			minGid = gid
			max = len(shard)
			maxGid = gid
		} else {
			if len(shard) > max {
				max = len(shard)
				maxGid = gid
			} else if len(shard) == max && gid > maxGid {
				maxGid = gid
			} else if len(shard) < min {
				min = len(shard)
				minGid = gid
			} else if len(shard) == min && gid < minGid {
				minGid = gid
			}
		}
		i++
	}
	return minGid, min, maxGid, max
}

func createGidShardMapFromConfig(config Config) map[int][]int {
	gidShardMap := make(map[int][]int)
	for k, _ := range config.Groups {
		gidShardMap[k] = []int{}
	}
	for k, v := range config.Shards {
		origin, ok := gidShardMap[v]
		if ok {
			gidShardMap[v] = append(origin, k)
		} else {
			minGid, _, _, _ := findMinMaxGid(gidShardMap)
			gidShardMap[minGid] = append(gidShardMap[minGid], k)
		}
	}
	return gidShardMap
}

/*
本实验的核心就是，reblance shard和groupID。shard是工作内容（工作目标），group是workers（groupID）
是标识这个群体的标志。shardController是动态调整它们之间的关系的程序。reblance尽量满足，
1. workers group 尽量都是有工作的，即shard有负责的group在做，体现在程序上就是，Shards [NShards]int ，这个变量，
是有真实的groupID（非0的groupID）的赋值；
2. Shards中的groupID尽量分布均匀，即各个groupID的数量相近，让每个shard（工作目标）都有差不多的worker在
解决
*/
func balanceShards(me int, config *Config) {
	DPrintf("%v: Balance config before %+v", me, config)
	if len(config.Groups) == 0 {
		// reset
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// only 1 gid
		for k, _ := range config.Groups {
			for i := 0; i < NShards; i++ {
				config.Shards[i] = k
			}
		}
	} else {
		gidShardMap := createGidShardMapFromConfig(*config)

		for {
			minGid, min, maxGid, max := findMinMaxGid(gidShardMap)
			if math.Abs(float64(max-min)) <= 1 {
				break
			}
			// start to balance
			source := gidShardMap[maxGid]
			// must sort
			sort.Ints(source)
			moveShard := source[len(source)-1]
			gidShardMap[maxGid] = source[:len(source)-1]
			target := gidShardMap[minGid]
			gidShardMap[minGid] = append(target, moveShard)
		}

		for gid, shards := range gidShardMap {
			for _, shard := range shards {
				config.Shards[shard] = gid
			}
		}
	}
	DPrintf("%v: Balance config after %+v", me, config)
}
func (sc *ShardCtrler) updateConfigWithoutLOCK(op Op) {
	switch op.Op {
	case 0:
		// Query
	case 1:
		// Join
		lastConfig := sc.queryWithoutLOCK(-1)
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Shards: lastConfig.Shards,
			Groups: deepCopyMap(lastConfig.Groups),
		}
		// add groups
		for gid, servers := range op.Servers {
			newConfig.Groups[gid] = servers
		}
		// balance shards
		balanceShards(sc.me, &newConfig)
		sc.configs = append(sc.configs, newConfig)
	case 2:
		// Leave
		lastConfig := sc.queryWithoutLOCK(-1)
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Shards: lastConfig.Shards,
			Groups: deepCopyMap(lastConfig.Groups),
		}
		for _, groupID := range op.GIDs {
			delete(newConfig.Groups, groupID)
		}
		// balance shards
		balanceShards(sc.me, &newConfig)
		sc.configs = append(sc.configs, newConfig)
	case 3:
		// Move
		config := sc.queryWithoutLOCK(-1)
		config.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, config)
	}
	sc.lastCommandID[op.ClientID] = op.CommandID
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case m := <-sc.applyCh:
			if m.SnapshotValid {

			} else if m.CommandValid {
				DPrintf("%d applyCh:%v\n", sc.me, m)
				op := m.Command.(Op)
				sc.mu.Lock()
				// 1. check commandID
				// 2. only the new one can update keyvalue db
				if sc.checkCommandIDWithoutLOCK(op) {
					// 2.1 update k-v
					sc.updateConfigWithoutLOCK(op)
					// 2.2 get command channel
					// 3. snapshot
					commandCh := sc.getCommandChanWithoutLOCK(op)
					sc.mu.Unlock()
					if commandCh != nil {
						DPrintf("[Server] <applier> %d enter rf.GetState clientID[%d] commandID[%d]\n", sc.me, op.ClientID, op.CommandID)
						// 2.3 only leader role can send data to channel
						if term, isLeader := sc.rf.GetState(); isLeader && op.Term >= term {
							DPrintf("[Server] <applier> %d sendback1 clientID[%d] commandID[%d]\n", sc.me, op.ClientID, op.CommandID)
							// commandCh <- op.CommandID
							select {
							case commandCh <- op.CommandID:
							case <-time.After(1 * time.Second):
							}
							DPrintf("[Server] <applier> %d sendback2 clientID[%d] commandID[%d]\n", sc.me, op.ClientID, op.CommandID)
						}
						sc.mu.Lock()
						sc.deleteCommandChanWithoutLOCK(op)
						sc.mu.Unlock()
					}
				} else {
					sc.mu.Unlock()
				}
			}
		}
	}
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastCommandID = make(map[int64]int64)
	sc.clientCh = make(map[int64]map[int64]chan int64)
	go sc.applier()
	return sc
}
