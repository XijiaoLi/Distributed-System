package shardkv

import "net"
// import "fmt"
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
  Shard int
}


type ShardKV struct {
  mu sync.Mutex
  seq_mu sync.Mutex
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
  old_req_memo map[int]map[ReqIndex]GeneralReply
  req_memo map[ReqIndex]GeneralReply
  db map[string]string
  last_seq int
  req_num int
  name string
  config_mu sync.RWMutex
  req_memo_mu sync.Mutex
  sync_mu sync.Mutex
  db_mu sync.Mutex
  newest_cfig_num int

}


// ------------------------------------------------

func (kv *ShardKV) update_config(latest int) {
  log.Printf("[%v] update_config wait mu *** \n", kv.name)
  kv.config_mu.Lock()
  log.Printf("[%v] update_config gett mu *** \n", kv.name)
  config := kv.last_cfig

  for config.Num < latest {
    if config.Num == 0 {
      config = kv.sm.Query(1)
			continue
		}

    log.Printf("[%v] update_config curr ConfigNum query === [%v]\n", kv.name, config.Num)
    next_cfig := kv.sm.Query(config.Num + 1)
    log.Printf("[%v] update_config curr ConfigNum query end === [%v]\n", kv.name, config.Num)

    db_copy := make(map[string]string)

    kv.db_mu.Lock()
    for k, v := range kv.db {
      if config.Shards[key2shard(k)] == kv.gid {
        db_copy[k] = v
      }
    }
    kv.db_mu.Unlock()

    kv.sync_mu.Lock()
    kv.db_memo[config.Num] = db_copy
    kv.sync_mu.Unlock()

    old_req_copy := make(map[ReqIndex]GeneralReply)

    kv.req_memo_mu.Lock()
    for req, rep := range kv.req_memo {
      if req.UUID != 0 {
        old_req_copy[req] = rep
      }
    }
    kv.req_memo_mu.Unlock()

    kv.sync_mu.Lock()
    kv.old_req_memo[config.Num] = old_req_copy
    kv.sync_mu.Unlock()

    log.Printf("[%v] update_config copied my db\n", kv.name)

    ga := make(map[int64]bool)

    for i, old_gid := range config.Shards {
      new_gid := next_cfig.Shards[i]
      if new_gid != old_gid && new_gid == kv.gid && !ga[old_gid] {
        log.Printf("[%v] update_config get data from old_gid[%v]\n", kv.name, old_gid)
        ga[old_gid] = true
        for done:=false; !done; {
          for _, old_server := range config.Groups[old_gid] {
            args := &SyncArgs{ConfigNum: config.Num}
            var reply SyncReply
            ok := call(old_server, "ShardKV.Sync", args, &reply)
            if ok && reply.Err == OK {
              kv.db_mu.Lock()
              for k, v := range reply.DBCopy {
                kv.db[k] = v
              }
              kv.db_mu.Unlock()

              kv.req_memo_mu.Lock()
              for req, rep := range reply.ReqCopy {
                kv.req_memo[req] = rep
              }
              kv.req_memo_mu.Unlock()

              done = true
              break
            }
          }
        }
      }
    }
    config = next_cfig
    log.Printf("[%v] new ConfigNum[%v]\n", kv.name, config.Num)
  } // end of for kv.last_cfig.Num < latest
  kv.last_cfig = config
  log.Printf("[%v] DONE! ConfigNum[%v]\n", kv.name, kv.last_cfig.Num)
  kv.config_mu.Unlock()
}

func (kv *ShardKV) interpret_log(op Op) {

  kv.req_memo_mu.Lock()
  _, existed := kv.req_memo[op.Req]
  kv.req_memo_mu.Unlock()

  if existed {
    return
  }

  var reply GeneralReply

  switch op.Operation {
  case GET:

    gid := kv.last_cfig.Shards[op.Shard]
    if gid != kv.gid {
      log.Printf("[%v] Log GET req%v key[%v] val[null] shard[%v] wg ConfigNum[%v]\n", kv.name, op.Req, op.Key, op.Shard, kv.last_cfig.Num)
  		reply.Err = ErrWrongGroup
  		break
  	}

    val, existed := kv.db[op.Key]
    if existed {
      reply.Value = val
      reply.Err = OK
    } else {
      reply.Err = ErrNoKey
    }

    log.Printf("[%v] Log GET req%v key[%v] val[%v] shard[%v] ok \n", kv.name, op.Req, op.Key, reply.Value, op.Shard)
  case PUT:

    gid := kv.last_cfig.Shards[op.Shard]
    if gid != kv.gid {
      log.Printf("[%v] Log PUT req%v key[%v] val[%v] shard[%v] wg \n", kv.name, op.Req, op.Key, op.Val, op.Shard)
  		reply.Err = ErrWrongGroup
  		break
  	}

    kv.db[op.Key] = op.Val
    reply.Err = OK

    log.Printf("[%v] Log PUT req%v key[%v] val[%v] shard[%v] ok \n", kv.name, op.Req, op.Key, op.Val, op.Shard)
  case PUTHASH:

    gid := kv.last_cfig.Shards[op.Shard]
    if gid != kv.gid {
      log.Printf("[%v] Log PHA req%v key[%v] val[%v] shard[%v] wg \n", kv.name, op.Req, op.Key, op.Val, op.Shard)
  		reply.Err = ErrWrongGroup
  		break
  	}

    reply.Value, _ = kv.db[op.Key]
    h := hash(reply.Value + op.Val)
    kv.db[op.Key] = strconv.Itoa(int(h))
    reply.Err = OK

    log.Printf("[%v]Log PHA req%v key[%v] val[%v]-[%v] shard[%v] ok \n", kv.name, op.Req, op.Key, op.Val, reply.Value, op.Shard)
  case CONFIG:
    log.Printf("[%v] Log UPD req%v\n", kv.name, op.Req)
    kv.update_config(op.ConfigNum)
    log.Printf("[%v] Log UPD req%v done! \n", kv.name, op.Req)
  }

  log.Printf("[%v] Log DONE for req%v\n", kv.name, op.Req)
  if reply.Err != ErrWrongGroup {
    log.Printf("[%v] Log LOG for req%v\n", kv.name, op.Req)
    kv.req_memo_mu.Lock()
    log.Printf("[%v] Log LOG for req%v DONE!\n", kv.name, op.Req)
    kv.req_memo[op.Req] = reply
    kv.req_memo_mu.Unlock()
  }
}

func (kv *ShardKV) catch_up(op Op) GeneralReply {
  log.Printf("[%v]     catch_up for req%v waiting for lock\n", kv.name, op.Req)

  var decided bool
  var logger interface{}
  seq := 0

  kv.mu.Lock()
  defer kv.mu.Unlock()

  for {
    kv.seq_mu.Lock()
    kv.last_seq ++
    seq = kv.last_seq
    kv.seq_mu.Unlock()

    decided, logger = kv.px.Status(seq)
    if !decided {
      kv.px.Start(seq, op)
      nap := 10 * time.Millisecond
      for {
        decided, logger = kv.px.Status(seq)
        if decided {
          break
        }
        time.Sleep(nap)
        if nap < 10 * time.Second {
          nap *= 2
        }
      }
    }
    op_temp, _ := logger.(Op)
    kv.interpret_log(op_temp)
    if op_temp.Req == op.Req {
      break
    }
  }

  log.Printf("[%v]     finished catch_up for req%v\n", kv.name, op.Req)

  kv.px.Done(seq-1)

  kv.req_memo_mu.Lock()
  log.Printf("[%v]     catch_up return val for req%v\n", kv.name, op.Req)
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
// ------------------------------------------------
// ------------------------------------------------


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.

  kv.req_memo_mu.Lock()
  result, existed := kv.req_memo[args.Req]
  kv.req_memo_mu.Unlock()

  log.Printf("[%v]     GET req%v key[%v] existed[%v]\n", kv.name, args.Req, args.Key, existed)

  if !existed {
    kv.config_mu.Lock()
    gid := kv.last_cfig.Shards[args.Shard]
    kv.config_mu.Unlock()

    if kv.gid == gid {
      op := Op{Operation: GET, Key: args.Key, Req: args.Req, Shard: args.Shard}
      result = kv.catch_up(op)
    } else {
      reply.Err = ErrWrongGroup
      return nil
    }
  }

  reply.Err = result.Err
  reply.Value = result.Value

  kv.req_memo_mu.Lock()
  kv.free_req(args.Req)
  kv.req_memo_mu.Unlock()

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {

  // Your code here.

  kv.req_memo_mu.Lock()
  result, existed := kv.req_memo[args.Req]
  kv.req_memo_mu.Unlock()

  log.Printf("[%v]     PUT req%v key[%v] val[%v] existed[%v]\n", kv.name, args.Req, args.Key, args.Value, existed)

  if !existed {
    kv.config_mu.Lock()
    gid := kv.last_cfig.Shards[args.Shard]
    kv.config_mu.Unlock()

    if kv.gid == gid {
      op := Op{Operation: PUT, Key: args.Key, Val: args.Value, Req: args.Req, Shard: args.Shard}
      if args.DoHash {
    		op.Operation = PUTHASH
    	}
      result = kv.catch_up(op)
      log.Printf("[%v]     PUT req%v key[%v] val[%v] existed[%v]\n", kv.name, args.Req, args.Key, args.Value, existed)
    } else {
      reply.Err = ErrWrongGroup
      return nil
    }
  }

  reply.Err = result.Err
  reply.PreviousValue = result.Value

  kv.req_memo_mu.Lock()
  kv.free_req(args.Req)
  kv.req_memo_mu.Unlock()

  return nil
}

func (kv *ShardKV) Sync(args *SyncArgs, reply *SyncReply) error {

  kv.sync_mu.Lock()
  db_copy, ok1 := kv.db_memo[args.ConfigNum]
  old_req_copy, ok2 := kv.old_req_memo[args.ConfigNum]
  kv.sync_mu.Unlock()

  if ok1 && ok2 {
		reply.DBCopy = db_copy
    reply.ReqCopy = old_req_copy
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

  log.Printf("[%v] tick \n", kv.name)

	latest_cfig := kv.sm.Query(-1)
  kv.config_mu.RLock()
  if latest_cfig.Num == kv.last_cfig.Num {
    kv.config_mu.RUnlock()
    log.Printf("[%v] tick no change\n", kv.name)
    return
  }
  kv.config_mu.RUnlock()

  req := ReqIndex{ReqNum: latest_cfig.Num, UUID: int64(0)}

  op := Op{Operation: CONFIG, ConfigNum: latest_cfig.Num, Req: req}
  log.Printf("[%v] tick catch up      --- \n", kv.name)
  kv.catch_up(op)
  log.Printf("[%v] tick catch up done ---\n", kv.name)

  kv.req_memo_mu.Lock()
  kv.free_req(req)
  kv.req_memo_mu.Unlock()
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
  kv.name = servers[me][28:]
  kv.old_req_memo = make(map[int]map[ReqIndex]GeneralReply)

  // Your initialization code here.
  // Don't call Join().
  kv.db_memo = make(map[int]map[string]string)
  kv.req_memo = make(map[ReqIndex]GeneralReply)
  kv.db = make(map[string]string)
  kv.last_seq = -1
  kv.newest_cfig_num = 0

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
      // fmt.Printf("S tick prep [%v] \n", kv.name)
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
