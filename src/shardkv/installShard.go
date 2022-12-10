package shardkv

import (
	"fmt"
	"time"

	"6.824/shardctrler"
)

type InstallShardArgs struct {
	ClientID  int64
	CommandID int64
	DB        shardKvDB
	ShardNum  int
	Config    shardctrler.Config
}
type InstallShardReply struct {
	Err Err
}

// send shard to matched gid servers
func (kv *ShardKV) SendShard(shardNum int, config shardctrler.Config) {
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	gid := config.Shards[shardNum]
	args := InstallShardArgs{
		ClientID:  kv.shardKvStore.id(shardNum),
		CommandID: kv.lastCommandID[kv.shardKvStore.id(shardNum)] + 1,
		DB:        kv.shardKvStore.shard(shardNum),
		ShardNum:  shardNum,
		Config:    config,
	}
	kv.mu.Unlock()
	for {
		_, isLeader1 := kv.rf.GetState()
		if !isLeader1 {
			return
		}
		ok := false
		kv.mu.Lock()
		for _, sh := range kv.deletingShard {
			if sh == shardNum {
				ok = true
				break
			}
		}
		kv.mu.Unlock()
		if !ok {
			return
		}
		fmt.Printf("SendShard gid:%d kv:%d config:%v At:%v\n", kv.gid, kv.me, config, time.Now())
		DPrintf("[Client] <SendShard> client[%d] send to gid[%d] try shard:%d db:%v commandID:%d\n", args.ClientID, gid, shardNum, args.DB, args.CommandID)
		if servers, ok := config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply InstallShardReply
				ok := srv.Call("ShardKV.InstallShard", &args, &reply)
				if ok && reply.Err == OK {
					kv.mu.Lock()
					kv.lastCommandID[args.ClientID] = args.CommandID
					kv.shardKvStore.setStatus(shardNum, ShardNormal)
					kv.mu.Unlock()
					return
				}
				// ... not ok, or ErrWrongLeader
				if ok && reply.Err == ErrWrongLeader {
					fmt.Printf("ErrWrongLeaderSendShard gid:%d kv:%d config:%v servers:%d\n", kv.gid, kv.me, config, si)
					continue
				}

				if ok && reply.Err == ErrShardWaiting {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
	}
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	command := Op{
		CommandType: InstallShardCommandType,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
		ShardTask:   args.ShardNum,
		DB:          args.DB,
		Config:      args.Config,
	}

	kv.mu.Lock()
	term, isLeader1 := kv.rf.GetState()
	if !isLeader1 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <InstallShard> gid:%d follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	if args.Config.Num <= kv.config.Num {
		kv.shardKvStore.setStatus(command.ShardTask, ShardNormal)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		kv.shardKvStore.setStatus(command.ShardTask, ShardNormal)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// 2. check leader role and append logEntry

	if kv.shardKvStore.status(args.ShardNum) != ShardWaiting {
		reply.Err = ErrShardWaiting
		fmt.Printf("[Server] <InstallShard> ErrShardWaiting gid:%d follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	command.Term = term
	_, term, isLeader2 := kv.rf.Start(command)
	if !isLeader1 || !isLeader2 {
		reply.Err = ErrWrongLeader
		DPrintf("[Server] <InstallShard> follower[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	DPrintf("[Server] <InstallShard> begin![%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	// 3. get command channel
	commandCh := kv.makeCommandChanWithoutLOCK(command)
	DPrintf("[Server] <InstallShard> UNLOCK[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
	kv.mu.Unlock()
	DPrintf("[Server] <InstallShard> WAIT commandCH[%d]! ClientID[%d] ComandID[%d]\n", kv.me, args.ClientID, args.CommandID)
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
	DPrintf("[Server] <InstallShard>finish! gid:%d me:%d  ClientID[%d] ComandID[%d] Err:%s\n", kv.gid, kv.me, args.ClientID, args.CommandID, reply.Err)
}
