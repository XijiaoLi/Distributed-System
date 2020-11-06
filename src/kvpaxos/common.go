package kvpaxos

import "hash/fnv"
import "crypto/rand"
import "math/big"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  PUT = "Put"
  PUTHASH = "PutHash"
  GET = "Get"
)
type Err string

type ReqIndex struct {
  UUID int64
  ReqNum int64
}

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
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
  Req ReqIndex
}

type GetReply struct {
  Err Err
  Value string
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
