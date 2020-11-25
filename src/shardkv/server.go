package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  Operation string
  Key string
  Val string
  ConfigNum int
  Req ReqIndex
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  last_cfig shardmaster.Config
  db_memo map[int]map[string]string
  req_memo map[ReqIndex]GeneralReply
  db map[string]string
  last_seq int
  req_num int
  name string
}


// ------------------------------------------------

func (kv *ShardKV) update_config(latest int) {
  for kv.last_cfig.Num < latest {
    if kv.last_cfig.Num == 0 {
			kv.last_cfig = kv.sm.Query(1)
			continue
		}

    db_copy := make(map[string]string)
    for k, v := range kv.db {
      if kv.last_cfig.Shards[key2shard(k)] == kv.gid {
        db_copy[k] = v
      }
    }
    kv.db_memo[kv.last_cfig.Num] = db_copy
    // kv.db_memo[kv.config.Num] = copyMapData(kv.seenOp)

    next_cfig := kv.sm.Query(kv.last_cfig.Num + 1)
    ga := make(map[int64]bool)

    for i, old_gid := range kv.last_cfig.Shards {
      new_gid := next_cfig.Shards[i]
      if new_gid != old_gid && new_gid == kv.gid {
        ga[old_gid] = true
      }
    }

    fmt.Printf("sync overview %v to [%v]\n", ga, kv.name)
    var wg sync.WaitGroup
    wg.Add(len(ga))

    fmt.Printf("sync start\n")
    for old_gid, _ := range ga {
      go func(old_gid int64){
        defer wg.Done()
        for done:=false; !done; {
          for _, old_server := range kv.last_cfig.Groups[old_gid] {
            fmt.Printf("sync g[%v] old [%v] new [%v]\n", old_gid, old_server, kv.name)
            args := &SyncArgs{ConfigNum: kv.last_cfig.Num}
            var reply SyncReply
            ok := call(old_server, "ShardKV.Sync", args, &reply)
            if ok && reply.Err == OK {
              for k, v := range reply.DBCopy {
                s := key2shard(k)
                if kv.last_cfig.Shards[s] == old_gid && next_cfig.Shards[s] == kv.gid {
                  kv.db[k] = v
                }
      				}
              // kv.addSeenOp(reply.SeenOp)
              done = true
              break
            }
          }
        }
      }(old_gid)
    }

    wg.Wait()
    kv.last_cfig = next_cfig
    fmt.Printf("sync end s[%v], new config[%v] \n", kv.name, kv.last_cfig.Num)
  } // end of for kv.last_cfig.Num < latest
  fmt.Printf("---------- sync finished\n")
}

func (kv *ShardKV) interpret_log(op Op) {
  var reply GeneralReply

  // // fmt.Printf("S[%v] OP: %v\n", kv.me, op)

  switch op.Operation {
  case GET:
    val, existed := kv.db[op.Key]
    if existed {
      reply.Value = val
      reply.Err = OK
    } else {
      reply.Value = ""
      reply.Err = ErrNoKey
    }
  case PUT:
    kv.db[op.Key] = op.Val
    reply.Err = OK
  case PUTHASH:
    reply.Value, _ = kv.db[op.Key]
    h := hash(reply.Value + op.Val)
    kv.db[op.Key] = strconv.Itoa(int(h))
    reply.Err = OK
  case CONFIG:
    kv.update_config(op.ConfigNum)
  }

  if op.Operation != CONFIG {
    kv.req_memo[op.Req] = reply
  }
}


func (kv *ShardKV) catch_up(op Op) GeneralReply {

  var decided bool
  var log interface{}

  for {
    kv.last_seq ++
    decided, log = kv.px.Status(kv.last_seq)
    if !decided {
      kv.px.Start(kv.last_seq, op)
      nap := 10 * time.Millisecond
      for {
        time.Sleep(nap)
        decided, log = kv.px.Status(kv.last_seq)
        if decided {
          break
        }
        if nap < 10 * time.Second {
          nap *= 2
        }
      }
    }
    op_temp, _ := log.(Op)
    kv.interpret_log(op_temp)
    if op_temp.Req == op.Req {
      break
    }
  }

  return kv.req_memo[op.Req]
}

func (kv *ShardKV) free_req(curr_req ReqIndex) {
	if curr_req.ReqNum-1 != 0 {
    prev := ReqIndex{ReqNum: curr_req.ReqNum-1, UUID: curr_req.UUID}
		_, ok := kv.req_memo[prev]
		if ok {
			delete(kv.req_memo, prev)
		}
	}
}



// ------------------------------------------------


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.

  fmt.Printf("S GET lock start s[%v] k[%v]\n", kv.name, args.Key)

  kv.mu.Lock()
	defer kv.mu.Unlock()
  fmt.Printf("S GET lock end\n")


  if kv.dead {
    // fmt.Printf("S GET: dead!!! [%v]\n", kv.name)
		return nil
	} else {
    // fmt.Printf("S GET: %v [%v]\n", kv.name, args.Key)
  }

	shard := key2shard(args.Key)
  // latest_config := kv.sm.Query(-1)

	if kv.last_cfig.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

  result, existed := kv.req_memo[args.Req]
  if !existed {
    op := Op{Operation: GET, Key: args.Key, Req: args.Req}
    fmt.Printf("S GET catchup begin\n")
    result = kv.catch_up(op)
    fmt.Printf("S GET catchup end\n")
  }

  reply.Err = result.Err
  reply.Value = result.Value
  // fmt.Printf("S GET: %v finished [%v]\n", kv.me, args.Key)

  kv.px.Done(kv.last_seq)
  kv.free_req(args.Req)

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.

  fmt.Printf("S PUT lock start [%v] \n", kv.name)

  kv.mu.Lock()
	defer kv.mu.Unlock()

  fmt.Printf("S PUT lock end [%v] \n", kv.name)

  if kv.dead {
    // fmt.Printf("S PUT dead [%v]!!!\n", kv.name)
		return nil
	} else {
    // fmt.Printf("S PUT: %v [%v]\n", kv.name, args.Key)
  }

	shard := key2shard(args.Key)

	if kv.last_cfig.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

  result, existed := kv.req_memo[args.Req]
  if !existed {
    op := Op{Operation: PUT, Key: args.Key, Val: args.Value, Req: args.Req}
    if args.DoHash {
  		op.Operation = PUTHASH
  	}
    fmt.Printf("S catch_up begin\n")
    result = kv.catch_up(op)
    fmt.Printf("S catch_up end\n")
  }

  reply.Err = result.Err
  reply.PreviousValue = result.Value

  kv.px.Done(kv.last_seq)
  kv.free_req(args.Req)

  return nil
}

func (kv *ShardKV) Sync(args *SyncArgs, reply *SyncReply) error {

  if db_copy, ok := kv.db_memo[args.ConfigNum]; ok {
		reply.DBCopy = db_copy
    reply.Err = OK
	} else {
    reply.Err = ErrNoDBCopy
  }
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
	defer kv.mu.Unlock()

	latest_cfig := kv.sm.Query(-1)
	if kv.last_cfig.Num != latest_cfig.Num {
    fmt.Printf("S tick: latest_cfig [%v] new_cfig [%v]\n", kv.last_cfig.Num, latest_cfig.Num)
		op := Op{Operation: CONFIG, ConfigNum: latest_cfig.Num, Req: ReqIndex{ReqNum: kv.req_num, UUID: int64(kv.me)}}
		kv.catch_up(op)
	}
  // fmt.Printf("S tick end [%v] \n", kv.name)
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.name = servers[me]

  // Your initialization code here.
  // Don't call Join().
  kv.db_memo = make(map[int]map[string]string)
  kv.req_memo = make(map[ReqIndex]GeneralReply)
  kv.db = make(map[string]string)
  kv.last_seq = -1
  kv.req_num = 0

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            // fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        // fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
