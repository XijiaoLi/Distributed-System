package shardkv
import "hash/fnv"
import "crypto/rand"
import "math/big"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrNoCache = "ErrNoCache"
  PUT = "Put"
  PUTHASH = "PutHash"
  GET = "Get"
  CONFIG = "Configure"
)
type Err string

type PutArgs struct {
  Key string
  Shard int
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Req ReqIndex
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Shard int
  Req ReqIndex
}

type GetReply struct {
  Err Err
  Value string
}

type SyncArgs struct {
  CacheIdx CacheIndex
}

type SyncReply struct {
  Err Err
  DBCopy map[string]string
  ReqCopy map[ReqIndex]GeneralReply
  // ReqCopy map[ReqIndex]GeneralReply
}

type ReqIndex struct {
  UUID int64
  ReqNum int
}

type GeneralReply struct {
  Err Err
  Value string
}

type CacheIndex struct {
  ConfigNum int
  GroupId int64
}

type Cache struct {
  DB map[string]string
  Req map[ReqIndex]GeneralReply
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
