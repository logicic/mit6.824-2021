package kvraft

import "time"

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrTimeOut      = "ErrTimeOut"
	ErrDuplicateReq = "ErrDuplicateRequest"

	ExecuteTimeout = 2 * time.Second
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	CommandID int64
	Term      int
}

type PutAppendReply struct {
	Err  Err
	Term int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	CommandID int64
	Term      int
}

type GetReply struct {
	Err   Err
	Value string
	Term  int
}

func max(num1, num2 int64) int64 {
	if num1 > num2 {
		return num1
	}
	return num2
}
