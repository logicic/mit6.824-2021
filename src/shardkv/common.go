package shardkv

import (
	"fmt"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrTimeOut      = "ErrTimeOut"
	ErrDuplicateReq = "ErrDuplicateRequest"
	ErrShardWaiting = "ErrShardWaiting"
	ErrSendAgain    = "ErrSendAgain"

	ExecuteTimeout = 1 * time.Second
)

// command type
const (
	ExecuteCommandType      = 300
	InstallShardCommandType = 301
	DeleteShardCommandType  = 302
	UpdateConfigCommandType = 303
)

// server for client status
// const (
// 	GroupServing  = 200
// 	GroupWaiting  = 201
// 	ConfigWaiting = 202
// )
const Waiting = -101
const (
	ShardNormal  = 0
	ShardWaiting = 15
	ShardSending = 16
)

type Err string
type shardKvStore []*shardKvDB

type shardKvDB struct {
	Shard   int
	ID      int64
	Status  int
	KvStore map[string]string
}

func (db *shardKvDB) get(key string) (value string, ok bool) {
	value, ok = db.KvStore[key]
	return
}

func (db *shardKvDB) insert(key, value string) {
	db.KvStore[key] = value
}

func (db *shardKvDB) clear() {
	db.KvStore = make(map[string]string)
	db.Status = ShardNormal
}

func newShardKvStore(shardNum int) shardKvStore {
	dbs := make(shardKvStore, shardNum)
	for id := range dbs {
		db := &shardKvDB{
			Shard:   id,
			KvStore: make(map[string]string),
			ID:      nrand(),
		}
		dbs[id] = db
	}
	return dbs
}

func (db shardKvStore) get(shard int, key string) (value string, ok bool) {
	value, ok = db[shard].get(key)
	return
}

func (db shardKvStore) put(shard int, key, value string) {
	db[shard].KvStore[key] = value
}

func (db shardKvStore) append(shard int, key, value string) {
	db[shard].KvStore[key] = db[shard].KvStore[key] + value
}

func (db shardKvStore) delete(shard int) {
	db[shard].clear()
}

func (db shardKvStore) shard(shard int) shardKvDB {
	returndb := shardKvDB{
		Shard:   shard,
		KvStore: make(map[string]string),
	}
	for k, v := range db[shard].KvStore {
		returndb.KvStore[k] = v
	}
	return returndb
}

func (db shardKvStore) install(shard int, sdb shardKvDB) {
	db[shard].Shard = shard
	for k, v := range sdb.KvStore {
		db[shard].KvStore[k] = v
	}
}

func (db shardKvStore) status(shard int) int {
	return db[shard].Status
}

func (db shardKvStore) clearStatus() {
	for shard := range db {
		db[shard].Status = ShardNormal
	}
}

func (db shardKvStore) checkStatus(status int) bool {
	for shard := range db {
		if db[shard].Status == status {
			return false
		}
	}
	return true
}

func (db shardKvStore) isNormal() bool {
	ok := true
	for shard := range db {
		if db[shard].Status != ShardNormal {
			ok = false
		}
	}
	return ok
}

func (db shardKvStore) id(shard int) int64 {
	return db[shard].ID
}

// func (db shardKvStore) allStatus() bool {
// 	for shard := range db {
// 		fmt.Printf("db[shard].Status:%v\n", db[shard].Status)
// 		if db[shard].Status == GroupWaiting {
// 			return false
// 		}
// 	}
// 	return true
// }
func (db shardKvStore) setStatus(shard int, status int) {
	db[shard].Status = status
}
func (db shardKvStore) print(gid, me int) {
	for i := range db {
		fmt.Printf("############## gid:%d kv:%d status:%v db[%d]:%v\n", gid, me, db[i].Status, db[i].Shard, db[i].KvStore)
	}
}

func (db shardKvStore) deepcopy(src shardKvStore) {
	for shard := range src {
		db[shard].ID = src[shard].ID
		db[shard].Shard = src[shard].Shard
		db[shard].Status = src[shard].Status
		for k, v := range src[shard].KvStore {
			db[shard].KvStore[k] = v
		}
	}
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	CommandID int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	CommandID int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}
