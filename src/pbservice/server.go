package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"


// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  mu sync.Mutex // lock
  last_view viewservice.View
  db map[string]string // kv store
  req_memo map[ReqIndex]string // old requests, responses
  need_update_backup bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
	defer pb.mu.Unlock()

  // check whether I am the correct server
  if args.BackupReq && (pb.last_view.Backup != pb.me) || !args.BackupReq && (pb.last_view.Primary != pb.me) {
    reply.Err = ErrWrongServer
    return nil
	}

  // check whether this is a duplicated request
  res, existed := pb.req_memo[ReqIndex{args.UUID, args.ReqNum}]
  if existed {
    reply.PreviousValue = res // get the value from cache directly
  } else {
    pb.put_val(args.Key, args.Value, args.DoHash, reply)
    pb.req_memo[ReqIndex{args.UUID, args.ReqNum}] = reply.PreviousValue // store this new request

    // forward this request to backup
    if !args.BackupReq && pb.last_view.Backup != "" {
      var backup_reply PutReply
  		ok := call(pb.last_view.Backup, "PBServer.Put", &PutArgs{Key: args.Key, Value: args.Value, DoHash: args.DoHash, BackupReq: true, ReqNum: args.ReqNum, UUID: args.UUID}, &backup_reply)
      if !ok || backup_reply.Err == ErrWrongServer {
        pb.get_curr_view()
      } else if reply.PreviousValue != backup_reply.PreviousValue {
        pb.need_update_backup = true // data inconsistent; need synchronization
      }
    }
  }
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
	defer pb.mu.Unlock()

  if args.BackupReq && (pb.last_view.Backup != pb.me) || !args.BackupReq && (pb.last_view.Primary != pb.me) {
    reply.Err = ErrWrongServer
    return nil
	}

  res, existed := pb.req_memo[ReqIndex{args.UUID, args.ReqNum}]
  if existed {
    reply.Value = res
  } else {
    pb.get_key(args.Key, reply)
    pb.req_memo[ReqIndex{args.UUID, args.ReqNum}] = reply.Value

    if !args.BackupReq && (pb.last_view.Backup != ""){
      var backup_reply GetReply
  		ok := call(pb.last_view.Backup, "PBServer.Get", &GetArgs{Key: args.Key, BackupReq: true, ReqNum: args.ReqNum, UUID: args.UUID}, &backup_reply)
  		if !ok || backup_reply.Err == ErrWrongServer {
        pb.get_curr_view()
      } else if reply.Value != backup_reply.Value {
        pb.need_update_backup = true
      }
    }
  }
	return nil
}

// SyncDb RPC implementation
func (pb *PBServer) SyncDb(args *SyncDbArgs, reply *SyncDbReply) error {
  pb.mu.Lock()
	defer pb.mu.Unlock()

	curr_view, _ := pb.vs.Ping(pb.last_view.Viewnum)
  pb.last_view = curr_view

	if curr_view.Backup != pb.me {
		reply.Err = ErrWrongServer
	} else {
    pb.db = args.Database
  	pb.req_memo = args.ReqMemo
  }

	return nil
}

//  put value by key to database
func (pb *PBServer) put_val(key string, val string, do_hash bool, reply *PutReply) error {
  if do_hash {
		reply.PreviousValue, _ = pb.db[key]
    h := hash(reply.PreviousValue + val)
		val = strconv.Itoa(int(h))
	}
  pb.db[key] = val
  return nil
}

//  get value by key from database
func (pb *PBServer) get_key(key string, reply *GetReply) error {
  val, ok := pb.db[key]
	if ok {
    reply.Value = val
	} else {
		reply.Err = ErrNoKey // return error and empty string if kv pair is not not present
	}
  return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
	defer pb.mu.Unlock()

  pb.get_curr_view()
  if pb.need_update_backup && pb.last_view.Backup != "" {
    pb.sync_db()
  }
}

func (pb *PBServer) get_curr_view() {
  curr_view, _ := pb.vs.Ping(pb.last_view.Viewnum)

  if curr_view.Viewnum != pb.last_view.Viewnum {
    if curr_view.Primary == pb.me && curr_view.Backup != "" && curr_view.Backup != pb.last_view.Backup {
      pb.need_update_backup = true
    }
    pb.last_view = curr_view
  }
}

func (pb *PBServer) sync_db() {
  var sync_reply SyncDbReply
  call(pb.last_view.Backup, "PBServer.SyncDb", &SyncDbArgs{Database: pb.db, ReqMemo: pb.req_memo}, &sync_reply) // send an RPC request
  pb.need_update_backup = false
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.last_view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
  pb.db = make(map[string]string)
  pb.req_memo = make(map[ReqIndex]string)
  pb.need_update_backup = false

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    } // end for loop
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }() // thread

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }() // thread

  return pb
}
