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
	fmt.Printf("[Server] <InstallShard> compare me[%d] sendergid:%d config[%d] recvgid[%d] config[%d]\n", kv.me, kv.gid, kv.config.Num, args.ClientID, args.Config.Num)
	// 1. check duplicate and out-date data
	if !kv.checkCommandIDWithoutLOCK(command) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// 2. check leader role and append logEntry

	if args.Config.Num != kv.config.Num {
		reply.Err = ErrShardWaiting
		fmt.Printf("[Server] <InstallShard> ErrShardWaiting gid:%d follower[%d]! ClientID[%d] ComandID[%d]\n", kv.gid, kv.me, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	if kv.shardKvStore.status(args.ShardNum) == ShardNormal {
		kv.mu.Unlock()
		reply.Err = OK
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
