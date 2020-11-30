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
  last_seq_mu sync.Mutex
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
  config_mu sync.Mutex
  req_memo_mu sync.Mutex
  db_mu sync.Mutex
  db_memo_mu sync.Mutex

}


// ------------------------------------------------

func (kv *ShardKV) update_config(latest int) {
  // log.Printf("Log UPD req%v\n", op.Req)
  kv.config_mu.Lock()
  config := kv.last_cfig

  for config.Num < latest {
    if config.Num == 0 {
      config = kv.sm.Query(1)
			continue
		}
    kv.db_memo_mu.Lock()
    _, existed := kv.db_memo[config.Num]
    kv.db_memo_mu.Unlock()

    if !existed {
      db_copy := make(map[string]string)
      kv.db_mu.Lock()
      for k, v := range kv.db {
        if config.Shards[key2shard(k)] == kv.gid {
          db_copy[k] = v
        }
      }
      kv.db_mu.Unlock()

      kv.db_memo_mu.Lock()
      kv.db_memo[config.Num] = db_copy
      kv.db_memo_mu.Unlock()
    }

    next_cfig := kv.sm.Query(config.Num + 1)
    ga := make(map[int64]bool)

    for i, old_gid := range config.Shards {
      new_gid := next_cfig.Shards[i]
      if new_gid != old_gid && new_gid == kv.gid && !ga[old_gid] {
        ga[old_gid] = true
        for done:=false; !done; {
          for _, old_server := range config.Groups[old_gid] {
            args := &SyncArgs{ConfigNum: config.Num}
            var reply SyncReply
            ok := call(old_server, "ShardKV.Sync", args, &reply)
            if ok && reply.Err == OK {
              for k, v := range reply.DBCopy {
                kv.db_mu.Lock()
                kv.db[k] = v
                kv.db_mu.Unlock()
      				}
              done = true
              break
            }
          }
        }
      }
    }
    config = next_cfig
  } // end of for kv.last_cfig.Num < latest
  kv.last_cfig = config
  kv.config_mu.Unlock()
}

func (kv *ShardKV) interpret_log(op Op) {
  f, err := os.OpenFile(strconv.Itoa(int(kv.gid)) + "-" + strconv.Itoa(kv.me), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if err != nil {
    log.Fatalf("error opening file: %v", err)
  }
  defer f.Close()
  log.SetOutput(f)

  var reply GeneralReply

  switch op.Operation {
  case GET:

    shard := key2shard(op.Key)

    kv.config_mu.Lock()
    gid := kv.last_cfig.Shards[shard]
    kv.config_mu.Unlock()

    if gid != kv.gid {
      log.Printf("Log GET req%v key[%v] val[null] shard[%v] wg \n", op.Req, op.Key, shard)
  		reply.Err = ErrWrongGroup
  		break
  	}

    kv.db_mu.Lock()
    val, existed := kv.db[op.Key]
    kv.db_mu.Unlock()

    if existed {
      reply.Value = val
      reply.Err = OK
    } else {
      reply.Value = ""
      reply.Err = ErrNoKey
    }

    log.Printf("Log GET req%v key[%v] val[%v] shard[%v] rg \n", op.Req, op.Key, reply.Value, shard)
  case PUT:
    shard := key2shard(op.Key)

    kv.config_mu.Lock()
    gid := kv.last_cfig.Shards[shard]
    kv.config_mu.Unlock()

    if gid != kv.gid {
      log.Printf("Log PUT req%v key[%v] val[%v] shard[%v] wg \n", op.Req, op.Key, op.Val, shard)
  		reply.Err = ErrWrongGroup
  		break
  	}

    kv.db_mu.Lock()
    kv.db[op.Key] = op.Val
    kv.db_mu.Unlock()

    log.Printf("Log PUT req%v key[%v] val[%v] shard[%v] rg \n", op.Req, op.Key, op.Val, shard)

    reply.Err = OK
  case PUTHASH:
    shard := key2shard(op.Key)

    kv.config_mu.Lock()
    gid := kv.last_cfig.Shards[shard]
    kv.config_mu.Unlock()

    if gid != kv.gid {
      log.Printf("Log PHA req%v key[%v] val[%v] shard[%v] wg \n", op.Req, op.Key, op.Val, shard)
  		reply.Err = ErrWrongGroup
  		break
  	}

    kv.db_mu.Lock()
    reply.Value, _ = kv.db[op.Key]
    h := hash(reply.Value + op.Val)
    kv.db[op.Key] = strconv.Itoa(int(h))
    kv.db_mu.Unlock()

    log.Printf("Log PHA req%v key[%v] val[%v]-[%v] shard[%v] wg \n", op.Req, op.Key, op.Val, reply.Value, shard)

    reply.Err = OK
  case CONFIG:
    log.Printf("Log UPD req%v\n", op.Req)
    kv.update_config(op.ConfigNum)
  }

  kv.req_memo_mu.Lock()
  kv.req_memo[op.Req] = reply
  kv.req_memo_mu.Unlock()
}

func (kv *ShardKV) catch_up(op Op) GeneralReply {

  var decided bool
  var log interface{}

  for {
    kv.last_seq_mu.Lock()
    kv.last_seq ++
    seq := kv.last_seq
    kv.last_seq_mu.Unlock()

    decided, log = kv.px.Status(seq)
    if !decided {
      kv.px.Start(seq, op)
      nap := 10 * time.Millisecond
      for {
        time.Sleep(nap)
        decided, log = kv.px.Status(seq)
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

  kv.req_memo_mu.Lock()
  ret := kv.req_memo[op.Req]
  kv.req_memo_mu.Unlock()

  return ret
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

  f, err := os.OpenFile(strconv.Itoa(int(kv.gid)) + "-" + strconv.Itoa(kv.me), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if err != nil {
    log.Fatalf("error opening file: %v", err)
  }
  defer f.Close()
  log.SetOutput(f)

  // Your code here.
  if kv.dead {
		return nil
	}

  log.Printf("Get: req[%v] key[%v]\n", args.Req, args.Key)

  shard := key2shard(args.Key)

  kv.config_mu.Lock()
  gid := kv.last_cfig.Shards[shard]
  kv.config_mu.Unlock()
  if gid != kv.gid {
    log.Printf("Get: req[%v] key[%v] error wrong group\n", args.Req, args.Key)
		reply.Err = ErrWrongGroup
		return nil
	}

  kv.mu.Lock()
  result, existed := kv.req_memo[args.Req]
  if !existed {
    op := Op{Operation: GET, Key: args.Key, Req: args.Req}
    result = kv.catch_up(op)
  }
  kv.mu.Unlock()

  reply.Err = result.Err
  reply.Value = result.Value
  log.Printf("Get: req[%v] key[%v] done val[%v]\n", args.Req, args.Key, result.Value)


  kv.last_seq_mu.Lock()
  kv.px.Done(kv.last_seq)
  kv.last_seq_mu.Unlock()

  kv.req_memo_mu.Lock()
  kv.free_req(args.Req)
  kv.req_memo_mu.Unlock()

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  f, err := os.OpenFile(strconv.Itoa(int(kv.gid)) + "-" + strconv.Itoa(kv.me), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if err != nil {
    log.Fatalf("error opening file: %v", err)
  }
  defer f.Close()
  log.SetOutput(f)

  // Your code here.
  if kv.dead {
		return nil
	}

  log.Printf("Put: req[%v] key[%v] hash[%v]\n", args.Req, args.Key, args.DoHash)

	shard := key2shard(args.Key)

  kv.config_mu.Lock()
  gid := kv.last_cfig.Shards[shard]
  kv.config_mu.Unlock()
  if gid != kv.gid {
    log.Printf("Put: req[%v] key[%v] hash[%v] error wrong group\n", args.Req, args.Key, args.DoHash)
		reply.Err = ErrWrongGroup
		return nil
	}

  kv.mu.Lock()
  result, existed := kv.req_memo[args.Req]
  if !existed {
    op := Op{Operation: PUT, Key: args.Key, Val: args.Value, Req: args.Req}
    if args.DoHash {
  		op.Operation = PUTHASH
  	}
    result = kv.catch_up(op)
  }
  kv.mu.Unlock()

  reply.Err = result.Err
  reply.PreviousValue = result.Value
  log.Printf("Put: req[%v] key[%v] hash[%v] done val[%v]\n", args.Req, args.Key, args.DoHash, result.Value)

  kv.last_seq_mu.Lock()
  kv.px.Done(kv.last_seq)
  kv.last_seq_mu.Unlock()

  kv.req_memo_mu.Lock()
  kv.free_req(args.Req)
  kv.req_memo_mu.Unlock()

  return nil
}

func (kv *ShardKV) Sync(args *SyncArgs, reply *SyncReply) error {

  kv.db_memo_mu.Lock()
  if db_copy, ok := kv.db_memo[args.ConfigNum]; ok {
    kv.db_memo_mu.Unlock()
		reply.DBCopy = db_copy
    reply.Err = OK
	} else {
    kv.db_memo_mu.Unlock()
    reply.Err = ErrNoDBCopy
  }
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

	latest_cfig := kv.sm.Query(-1)

  kv.config_mu.Lock()
  config_id := kv.last_cfig.Num
  kv.config_mu.Unlock()



	if config_id != latest_cfig.Num {
    fmt.Printf("\n-------------------- shard --------------------\n>>> %v\n", latest_cfig.Shards)
    req := ReqIndex{ReqNum: latest_cfig.Num, UUID: int64(0)}
    kv.mu.Lock()
    _, existed := kv.req_memo[req]
    if !existed {
      op := Op{Operation: CONFIG, ConfigNum: latest_cfig.Num, Req: req}
      kv.catch_up(op)
    }
    kv.mu.Unlock()
	}
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
      fmt.Printf("S tick prep [%v] \n", kv.name)
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
