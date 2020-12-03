package shardmaster

import (
  "net"
  "fmt"
  "net/rpc"
  "log"
  "paxos"
  "sync"
  "os"
  "syscall"
  "encoding/gob"
  "math/big"
  "math/rand"
  "time"
  crand "crypto/rand"
)

type ShardMaster struct {
  mu sync.RWMutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  gid_nshards_counter map[int64]int // gid -> # of shards
  last_seq int
}


type Op struct {
  // Your data here.
  Operation string
  GroupId int64
  ShardId int
  Servers []string
  ConfigNum int
  Req int64
}

const (
  JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
  QUERY = "QUERY"
)

// --------------------------------------------------

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) init_new_config() *Config {
	new_config := sm.configs[len(sm.configs) - 1]
  new_config.Num = len(sm.configs)
  sm.configs = append(sm.configs, new_config)

	return &new_config
}

func (sm *ShardMaster) rebalance_join(config Config, joined_gid int64) Config {

  if group_num := len(config.Groups); group_num == 1 {
    sm.gid_nshards_counter[joined_gid] = NShards
    for i:=0; i<NShards; i++ {
      config.Shards[i] = joined_gid
    }
  } else {

    min_n := NShards/group_num
    max_n := min_n
    if NShards%group_num > 0 {
      max_n += 1
    }

    sm.gid_nshards_counter[joined_gid] = 0

    for shard, gid := range config.Shards {
      if sm.gid_nshards_counter[gid] > max_n {
        config.Shards[shard] = joined_gid
        sm.gid_nshards_counter[joined_gid] += 1
        sm.gid_nshards_counter[gid] -= 1
      }
    }

    if sm.gid_nshards_counter[joined_gid] < min_n {
      for shard, gid := range config.Shards {
        if sm.gid_nshards_counter[gid] > min_n {
          config.Shards[shard] = joined_gid
          sm.gid_nshards_counter[joined_gid] += 1
          sm.gid_nshards_counter[gid] -= 1
          if sm.gid_nshards_counter[joined_gid] == min_n {
            break
          }
        }
      }
    }
  }

  return config
}

func (sm *ShardMaster) rebalance_leave(config Config, left_gid int64) Config {

  if group_num := len(config.Groups); group_num == 0 {
    for i:=0; i<NShards; i++ {
      config.Shards[i] = 0
    }
    delete(sm.gid_nshards_counter, left_gid)
  } else {

    if sm.gid_nshards_counter[left_gid] != 0 {
      delete(sm.gid_nshards_counter, left_gid)
      min_n := NShards/group_num
      max_n := min_n
      if NShards%group_num > 0 {
        max_n += 1
      }

      var shards []int
      var groups []int64

      for shard, gid := range config.Shards {
        if gid == left_gid {
          shards = append(shards, shard)
        }
      }

      for gid, n := range sm.gid_nshards_counter {
        for ; n < min_n; n++ {
          groups = append([]int64{gid}, groups...)
        }
        for ; n < max_n; n++ {
          groups = append(groups, gid)
        }
      }

      for i := 0; i<len(shards); i++ {
        config.Shards[shards[i]] = groups[i]
        sm.gid_nshards_counter[(groups[i])] = 1 + sm.gid_nshards_counter[(groups[i])]
      }
    } else {
      delete(sm.gid_nshards_counter, left_gid)
    }

  }

  return config
}

func copy_config (config Config) Config {
  new_config := Config{Num: config.Num+1}
  for i, gid := range config.Shards {
      new_config.Shards[i] = gid
  }
  new_config.Groups = make(map[int64][]string)
  for g, servers := range config.Groups {
      new_config.Groups[g] = servers
  }
  return new_config
}

func (sm *ShardMaster) interpret_log(op Op) {

  switch op.Operation {
	case JOIN:
    new_config := copy_config(sm.configs[len(sm.configs) - 1])
    if _, ok := new_config.Groups[op.GroupId]; !ok {
      new_config.Groups[op.GroupId] = op.Servers
      new_config = sm.rebalance_join(new_config, op.GroupId)
    }
    sm.configs = append(sm.configs, new_config)
	case LEAVE:
    new_config := copy_config(sm.configs[len(sm.configs) - 1])
    if _, ok := new_config.Groups[op.GroupId]; ok {
      delete(new_config.Groups, op.GroupId)
      new_config = sm.rebalance_leave(new_config, op.GroupId)
    }
    sm.configs = append(sm.configs, new_config)
	case MOVE:
    new_config := copy_config(sm.configs[len(sm.configs) - 1])
    old_gid := new_config.Shards[op.ShardId]
    new_config.Shards[op.ShardId] = op.GroupId
    sm.configs = append(sm.configs, new_config)
    sm.gid_nshards_counter[old_gid] -= 1
    sm.gid_nshards_counter[op.GroupId] += 1
  } // end of switch
  // fmt.Printf("\n-----\n")
  // for _,c := range sm.configs {
  //   fmt.Printf("%v ", c.Num)
  // }
  // fmt.Printf("\n-----\n")
}

func (sm *ShardMaster) catch_up (op Op) {

  var decided bool
  var log interface{}

  for {
    sm.last_seq ++
    decided, log = sm.px.Status(sm.last_seq)
    if !decided {
      sm.px.Start(sm.last_seq, op)
      nap := 10 * time.Millisecond
      for {
        decided, log = sm.px.Status(sm.last_seq)
        if decided {
          break
        }
        time.Sleep(nap)
        if nap < 10 * time.Second {
          nap *= 2
        }
      }
    }
    op_temp, _ := log.(Op)
    sm.interpret_log(op_temp)
    if op_temp.Req == op.Req {
      break
    }
  }
}

// --------------------------------------------------

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
	defer sm.mu.Unlock()

  op := Op{Operation: JOIN, GroupId: args.GID, Servers: args.Servers, Req: nrand()}
  sm.catch_up(op)

  sm.px.Done(sm.last_seq)

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
	defer sm.mu.Unlock()

  op := Op{Operation: LEAVE, GroupId: args.GID, Req: nrand()}
  sm.catch_up(op)

  sm.px.Done(sm.last_seq)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
	defer sm.mu.Unlock()

  op := Op{Operation: MOVE, GroupId: args.GID, ShardId: args.Shard, Req: nrand()}
  sm.catch_up(op)

  sm.px.Done(sm.last_seq)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.

  sm.mu.RLock()

  if args.Num >= 0 && args.Num < len(sm.configs) {
    reply.Config = sm.configs[args.Num]
    sm.mu.RUnlock()
    return nil
  }

  sm.mu.RUnlock()

  op := Op{Operation: QUERY, ConfigNum: args.Num, Req: nrand()}

  sm.mu.Lock()
	defer sm.mu.Unlock()

  sm.catch_up(op)

  if op.ConfigNum >= 0 && op.ConfigNum < len(sm.configs) {
    reply.Config = sm.configs[op.ConfigNum]
  } else {
    reply.Config = sm.configs[len(sm.configs) - 1]
  }

  sm.px.Done(sm.last_seq)

  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.gid_nshards_counter = make(map[int64]int)
  sm.last_seq = 0

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
