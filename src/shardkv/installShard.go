package shardkv

import (
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
		ConfigNum:   args.Config.Num,
	}

	kv.mu.Lock()
	term, isLeader1 := kv.rf.GetState()
	if !isLeader1 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("[Server] <InstallShard> compare me[%d] recvgid:%d config[%d] sendergid[%d] config[%d]\n", kv.me, kv.gid, kv.config.Num, args.Config.Shards[args.ShardNum], args.Config.Num)
	// 1. check duplicate and out-date data
	if !kv.shardKvStore.checkCommandIDWithoutLOCK(command.ShardTask, command.ClientID, command.CommandID) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// 2. check leader role and append logEntry

	if args.Config.Num > kv.config.Num {
		reply.Err = ErrShardWaiting
		kv.mu.Unlock()
		return
	}
	if args.Config.Num < kv.config.Num {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	if kv.shardKvStore.status(args.ShardNum) == ShardNormal {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
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
	case <-commandCh:
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	DPrintf("[Server] <InstallShard>finish! gid:%d me:%d  ClientID[%d] ComandID[%d] Err:%s\n", kv.gid, kv.me, args.Config.Shards[args.ShardNum], args.CommandID, reply.Err)
}
