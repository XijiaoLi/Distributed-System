package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
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
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Operation string
  Key string
  Val string
  Req ReqIndex
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  req_memo map[ReqIndex]GeneralReply
  db map[string]string
  last_seq int
}

type GeneralReply struct {
  Err Err
  Value string
}

func (kv *KVPaxos) interpret_log(op Op) {
  var reply GeneralReply

  if op.Operation == GET {
    val, existed := kv.db[op.Key]
    if existed {
      reply.Value = val
      reply.Err = OK
    } else {
      reply.Value = ""
      reply.Err = ErrNoKey
    }
  } else if op.Operation == PUT {
    kv.db[op.Key] = op.Val
    reply.Err = OK
  } else if op.Operation == PUTHASH {
    reply.Value, _ = kv.db[op.Key]
    h := hash(reply.Value + op.Val)
    kv.db[op.Key] = strconv.Itoa(int(h))
    reply.Err = OK
  }

  kv.req_memo[op.Req] = reply
}


func (kv *KVPaxos) catch_up(op Op) GeneralReply {

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

func (kv *KVPaxos) free_req(curr_req ReqIndex) {
	if curr_req.ReqNum-1 != 0 {
    prev := ReqIndex{ReqNum: curr_req.ReqNum-1, UUID: curr_req.UUID}
		_, ok := kv.req_memo[prev]
		if ok {
			delete(kv.req_memo, prev)
		}
	}
}


// ----------------------------------------------------------------------

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
	defer kv.mu.Unlock()

  result, existed := kv.req_memo[args.Req]
  if !existed {
    op := Op{Operation: GET, Key: args.Key, Req: args.Req}
    result = kv.catch_up(op)
  }

  reply.Err = result.Err
  reply.Value = result.Value

  kv.px.Done(kv.last_seq)
  kv.free_req(args.Req)
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
	defer kv.mu.Unlock()

  result, existed := kv.req_memo[args.Req]
  if !existed {
    op := Op{Operation: PUT, Key: args.Key, Val: args.Value, Req: args.Req}
    if args.DoHash {
  		op.Operation = PUTHASH
  	}
    result = kv.catch_up(op)
  }

  reply.Err = result.Err
  reply.PreviousValue = result.Value

  kv.px.Done(kv.last_seq)
  kv.free_req(args.Req)
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
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
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}
