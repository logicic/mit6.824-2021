package shardkv

import "time"

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

	ExecuteTimeout = 1 * time.Second
)

// command type
const (
	ExecuteCommandType      = 100
	InstallShardCommandType = 101
	DeleteShardCommandType  = 102
	UpdateConfigCommandType = 103
)

type Err string
type shardKvStore []*shardKvDB

type shardKvDB struct {
	shardID int
	kvStore map[string]string
}

func (db *shardKvDB) get(key string) (value string, ok bool) {
	value, ok = db.kvStore[key]
	return
}

func (db *shardKvDB) insert(key, value string) {
	db.kvStore[key] = value
}

func newShardKvStore(shardNum int) shardKvStore {
	dbs := make(shardKvStore, shardNum)
	for id := range dbs {
		db := &shardKvDB{
			shardID: id,
			kvStore: make(map[string]string),
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
	db[shard].kvStore[key] = value
}

func (db shardKvStore) append(shard int, key, value string) {
	db[shard].kvStore[key] = db[shard].kvStore[key] + value
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
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	CommandID int64
}

type GetReply struct {
	Err   Err
	Value string
}
