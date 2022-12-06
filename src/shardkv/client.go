package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leader        int
	clientID      int64
	nextCommandID int64
	mu            sync.Mutex
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientID = nrand()
	DPrintf("ck.clientID:%d\n", ck.clientID)
	ck.nextCommandID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		// fmt.Printf("[Client] <Get> gid:%d kv:%d config:%v At:%v\n", gid, args.ClientID, ck.config, time.Now())
		DPrintf("[Client] <Get> client[%d] gid[%d] try shard:%d key:%s commandID:%d\n", ck.clientID, gid, shard, key, args.CommandID)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				if srv.Call("ShardKV.Get", &args, &reply) {
					if reply.Err == OK || reply.Err == ErrNoKey {
						ck.nextCommandID++
						return reply.Value
					}
					if reply.Err == ErrWrongGroup {
						break
					}
					// ... not ok, or ErrWrongLeader
					if reply.Err == ErrWrongLeader {
						DPrintf("[Client] <Get> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
						continue
					}

					if reply.Err == ErrShardWaiting {
						break
					}

					if reply.Err == ErrSendAgain {
						ck.nextCommandID++
						args.CommandID = ck.nextCommandID
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		// fmt.Printf("[Client] <PutAppend> gid:%d kv:%d config:%v At:%v\n", gid, args.ClientID, ck.config, time.Now())
		DPrintf("[Client] <PutAppend> client[%d] gid[%d] try shard:%d key:%s value:%s commandID:%d\n", ck.clientID, gid, shard, key, value, args.CommandID)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.nextCommandID++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && reply.Err == ErrWrongLeader {
					DPrintf("[Client] <PutAppend> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
					continue
				}

				if ok && reply.Err == ErrShardWaiting {
					break
				}

				if reply.Err == ErrSendAgain {
					ck.nextCommandID++
					args.CommandID = ck.nextCommandID
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
