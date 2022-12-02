package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader        int
	clientID      int64
	nextCommandID int64
	mu            sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leader = mrand.Intn(len(ck.servers))
	ck.clientID = nrand()
	DPrintf("ck.clientID:%d\n", ck.clientID)
	ck.nextCommandID = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{
		Num:       num,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}
	reply := &QueryReply{}
	for {
		if ck.servers[ck.leader].Call("ShardCtrler.Query", args, reply) {
			DPrintf("[Client] <Query> client[%d] mesg from server[%d] command[%d] Err:%v\n", ck.clientID, ck.leader, ck.nextCommandID, reply.Err)
			if reply.Err == OK {
				ck.nextCommandID++
				DPrintf("[Client] <Query> client[%d] server[%d] is leader config:%v\n", ck.clientID, ck.leader, reply.Config)
				return reply.Config
			}
			if reply.Err == ErrWrongLeader {
				ck.leader = mrand.Intn(len(ck.servers))
				DPrintf("[Client] <Query> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
				continue
			}
		} else {
			ck.leader = mrand.Intn(len(ck.servers))
			DPrintf("[Client] <Query> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
			continue
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	args := &JoinArgs{
		Servers:   servers,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}
	ck.mu.Lock()
	defer ck.mu.Unlock()
	reply := &JoinReply{}
	for {
		DPrintf("[Client] <Join> client[%d] client send to server[%d] command[%d] %v\n", ck.clientID, ck.leader, ck.nextCommandID, args)
		if ck.servers[ck.leader].Call("ShardCtrler.Join", args, reply) {
			DPrintf("[Client] <Join> client[%d] mesg from server[%d] command[%d] Err:%v\n", ck.clientID, ck.leader, ck.nextCommandID, reply.Err)
			if reply.Err == OK {
				ck.nextCommandID++
				DPrintf("[Client] <Join> client[%d] server[%d] is leader\n", ck.clientID, ck.leader)
				return
			}
			if reply.Err == ErrWrongLeader {
				ck.leader = mrand.Intn(len(ck.servers))
				DPrintf("[Client] <Join> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
				continue
			}
		} else {
			ck.leader = mrand.Intn(len(ck.servers))
			DPrintf("[Client] <Join> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
			continue
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	args := &LeaveArgs{
		GIDs:      gids,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}

	ck.mu.Lock()
	defer ck.mu.Unlock()
	reply := &LeaveReply{}
	for {
		DPrintf("[Client] <Leave> client[%d] client send to server[%d] command[%d] %v\n", ck.clientID, ck.leader, ck.nextCommandID, args)
		if ck.servers[ck.leader].Call("ShardCtrler.Leave", args, reply) {
			DPrintf("[Client] <Leave> client[%d] mesg from server[%d] command[%d] Err:%v\n", ck.clientID, ck.leader, ck.nextCommandID, reply.Err)
			if reply.Err == OK {
				ck.nextCommandID++
				DPrintf("[Client] <Leave> client[%d] server[%d] is leader\n", ck.clientID, ck.leader)
				return
			}
			if reply.Err == ErrWrongLeader {
				ck.leader = mrand.Intn(len(ck.servers))
				DPrintf("[Client] <Leave> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
				continue
			}
		} else {
			ck.leader = mrand.Intn(len(ck.servers))
			DPrintf("[Client] <Leave> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
			continue
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}
	ck.mu.Lock()
	defer ck.mu.Unlock()
	reply := &MoveReply{}
	for {
		DPrintf("[Client] <Move> client[%d] client send to server[%d] command[%d] %v\n", ck.clientID, ck.leader, ck.nextCommandID, args)
		if ck.servers[ck.leader].Call("ShardCtrler.Move", args, reply) {
			DPrintf("[Client] <Move> client[%d] mesg from server[%d] command[%d] Err:%v\n", ck.clientID, ck.leader, ck.nextCommandID, reply.Err)
			if reply.Err == OK {
				ck.nextCommandID++
				DPrintf("[Client] <Move> client[%d] server[%d] is leader\n", ck.clientID, ck.leader)
				return
			}
			if reply.Err == ErrWrongLeader {
				ck.leader = mrand.Intn(len(ck.servers))
				DPrintf("[Client] <Move> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
				continue
			}
		} else {
			ck.leader = mrand.Intn(len(ck.servers))
			DPrintf("[Client] <Move> client[%d] server[%d] try leader\n", ck.clientID, ck.leader)
			continue
		}
		time.Sleep(50 * time.Microsecond)
	}
}
