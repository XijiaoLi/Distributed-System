package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  sm *shardmaster.Clerk
  config shardmaster.Config
  // You'll have to modify Clerk.
  uuid int64
  req_num int
}



func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  // You'll have to modify MakeClerk.
  ck.uuid = nrand()
  ck.req_num = 0
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

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
  shard %= shardmaster.NShards
  return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Get().
  ck.req_num += 1
  args := &GetArgs{Key: key, Req: ReqIndex{ReqNum: ck.req_num, UUID: ck.uuid}}

  for {
    // ck.config = ck.sm.Query(-1)
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        var reply GetReply
        fmt.Printf("C GET srv[%v] key: %v\n", srv, args.Key)
        ok := call(srv, "ShardKV.Get", args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          fmt.Printf("C GET srv[%v] successfully \n", srv)
          return reply.Value
        }
        if ok && (reply.Err == ErrWrongGroup) {
          fmt.Printf("C GET srv[%v] ErrWrongGroup \n", srv)
          break
        }
        if !ok {
          fmt.Printf("C GET srv[%v] done\n", srv)
        }
      }
    }

    time.Sleep(10 * time.Millisecond)

    // ask master for a new configuration.
    fmt.Printf("C query begin\n")
    ck.config = ck.sm.Query(-1)
    fmt.Printf("C query end\n")

  }
  return ""
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  fmt.Printf("---------- client put lock start\n")
  ck.mu.Lock()
  defer ck.mu.Unlock()
  fmt.Printf("---------- client put lock end\n")

  // You'll have to modify Put().
  ck.req_num += 1
	args := &PutArgs{Key: key, Value: value, DoHash: dohash, Req: ReqIndex{ReqNum: ck.req_num, UUID: ck.uuid}}

  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      fmt.Printf("---------- client put start\n")
      // try each server in the shard's replication group.
      for _, srv := range servers {
        var reply PutReply
        fmt.Printf("C PUT srv[%v] key: %v\n", srv, args.Key)
        ok := call(srv, "ShardKV.Put", args, &reply)
        if ok && reply.Err == OK {
          fmt.Printf("---------- client put end\n")
          return reply.PreviousValue
        }
        if ok && (reply.Err == ErrWrongGroup) {
          fmt.Printf("---------- client put end\n")
          break
        }
        fmt.Printf("---------- client put change another server\n")
      }
    }


    time.Sleep(10 * time.Millisecond)

    // ask master for a new configuration.
    fmt.Printf("C query begin\n")
    ck.config = ck.sm.Query(-1)
    fmt.Printf("C query end\n")
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
