package kvraft

import (
	"math/big"
	"sync"

	"crypto/rand"
	mrand "math/rand"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.leader = -1
	ck.clientID = nrand()
	DPrintf("ck.clientID:%d\n", ck.clientID)
	ck.nextCommandID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}
	reply := GetReply{}
	if ck.leader != -1 {
		if ck.servers[ck.leader].Call("KVServer.Get", &args, &reply) {
			DPrintf("[Client] <Get> mesg from server[%d] Err:%v\n", ck.leader, reply.Err)
			if reply.Err == OK {
				ck.nextCommandID++
				return reply.Value
			}
		}
	}
	for {
		i := mrand.Intn(len(ck.servers))
		if ck.servers[i].Call("KVServer.Get", &args, &reply) {
			DPrintf("[Client] <Get> mesg from server[%d] Err:%v\n", i, reply.Err)
			if reply.Err == OK {
				ck.leader = i
				DPrintf("[Client] <Get> server[%d] is leader\n", i)
				ck.nextCommandID++
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				ck.leader = i
				DPrintf("[Client] <Get> server[%d] is leader\n", i)
				ck.nextCommandID++
				return ""
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		CommandID: ck.nextCommandID,
	}
	reply := PutAppendReply{}
	if ck.leader != -1 {
		DPrintf("[Client] <PutAppend> first client send to server[%d] %v\n", ck.leader, args)
		if ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply) {
			DPrintf("[Client] <PutAppend> mesg from server[%d] Err:%v\n", ck.leader, reply.Err)
			if reply.Err == OK {
				ck.nextCommandID++
				return
			}
		}
	}
	for {
		i := mrand.Intn(len(ck.servers))
		DPrintf("[Client] <PutAppend> client send to server[%d] %v\n", i, args)
		if ck.servers[i].Call("KVServer.PutAppend", &args, &reply) {
			DPrintf("[Client] <PutAppend> mesg from server[%d] Err:%v\n", i, reply.Err)
			if reply.Err == OK {
				ck.leader = i
				ck.nextCommandID++
				DPrintf("[Client] <PutAppend> server[%d] is leader\n", i)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
