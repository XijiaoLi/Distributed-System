package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.

  // Field names must start with capital letters,
  // otherwise RPC will break.
  BackupReq bool
  UUID int64
  ReqNum int64
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  BackupReq bool
  UUID int64
  ReqNum int64
}

type GetReply struct {
  Err Err
  Value string
}

type ReqIndex struct {
  UUID int64
  ReqNum int64
}

type SyncDbArgs struct {
	Database map[string]string
  ReqMemo map[ReqIndex]string
}

type SyncDbReply struct {
	Err Err
}

// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
