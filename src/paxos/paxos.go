package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

const (
	OK       = "OK"
	REJECT   = "REJECT"
)



type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  total int
  last_min int
  ins_memo map[int]Instance
  peers_min_done map[int]int

}

type Instance struct {
  n_p int
  n_a int
  v_a interface{}
  decided bool
}

type PrepareArgs struct {
  Seq int
  N int
}

type PrepareReply struct {
  Status string
  Na int
  Va interface{}
  NpHint int
}

type AcceptArgs struct {
  Seq int
  N int
  V interface{}
}

type AcceptReply struct {
  Status string
  NpHint int
}

type DecideArgs struct {
  Seq int
  V interface{}
	Peer int
	Done int
}

type DecideReply struct {
  Status string
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  px.mu.Lock()
  min := px.last_min
  px.mu.Unlock()
  if seq < min {
    return
  }
  // log.Printf("Paxos[%v] Start seq[%v] ---------- \n", px.me, seq) //## DEBUG
  go func() {
    n := px.pick_n(seq)
    // log.Printf("     pick n: %v \n", n) //## DEBUG
    for !px.dead {
      succeed, va, n_h := px.send_prepare(seq, n)
      // log.Printf("     1. propose - success: %v, va: %v, nh: %v \n", succeed, va, n_h) //## DEBUG
      if succeed {
        if va == nil {
          va = v
        }
        succeed, n_h = px.send_accept(seq, n, va)
        // log.Printf("     2. accept - success: %v, nh: %v \n", succeed, n_h) //## DEBUG
        if succeed {
          px.send_decide(seq, n, va)
          // log.Printf("     3. decided \n") //## DEBUG
          break
        }
      }
      n = px.cal_proposal_num(n_h)
      // log.Printf("     repick n: %v\n", n) //## DEBUG
      time.Sleep(time.Duration(rand.Intn(10)+1)*time.Millisecond)
    }
    // log.Printf("Paxos[%v] Start-finished ---------- \n", px.me) //## DEBUG
    return
  }()

}

func (px *Paxos) send_prepare(seq int, n int) (bool, interface{}, int){
  prepare_args := PrepareArgs{Seq: seq, N: n}
  prepare_reply_chan := make(chan *PrepareReply)

  for i, peer := range px.peers {
    go func(i int, peer string) {
      var prepare_reply PrepareReply
      if i != px.me {
        ok := call(peer, "Paxos.Prepare", &prepare_args, &prepare_reply)
        if ok {
          prepare_reply_chan <- &prepare_reply
        } else {
          prepare_reply_chan <- nil
        }
      } else {
        px.Prepare(&prepare_args, &prepare_reply)
  			prepare_reply_chan <- &prepare_reply
      }
    }(i, peer)
  }

  success := 0
  max_na := -1
  var val interface{}
  for _ = range px.peers {
  	re := <-prepare_reply_chan
  	if re != nil && re.Status == OK {
      success ++
			if re.Va != nil && re.Na > max_na { // get the hignest accepted value
				max_na = re.Na
				val = re.Va
			}
		} else if re != nil && re.Status == REJECT {
      if n < re.NpHint {
        n = re.NpHint
      }
    }
	}

	if success > len(px.peers)/2 {
		return true, val, n
	} else {
    return false, nil, n
  }
}

func (px *Paxos) send_accept(seq int, n int, v interface{}) (bool, int){
  accept_args := AcceptArgs{Seq: seq, N: n, V: v}
  accept_reply_chan := make(chan *AcceptReply)
  for i, peer := range px.peers {
    go func(i int, peer string) {
      var accept_reply AcceptReply
      if i != px.me {
        ok := call(peer, "Paxos.Accept", &accept_args, &accept_reply)
        if ok {
          accept_reply_chan <- &accept_reply
        } else {
          accept_reply_chan <- nil
        }
      } else {
        px.Accept(&accept_args, &accept_reply)
  			accept_reply_chan <- &accept_reply
      }
    }(i, peer)
  }

  success := 0
  for _ = range px.peers {
  	re := <-accept_reply_chan
  	if re != nil && re.Status == OK {
      success ++
		} else if re != nil && re.Status == REJECT {
      if n < re.NpHint {
        n = re.NpHint
      }
    }
	}

  if success > len(px.peers)/2 {
		return true, n
	} else {
    return false, n
  }
}

func (px *Paxos) send_decide(seq int, n int, v interface{}) {
  decide_args := DecideArgs{Seq: seq, V: v, Peer: px.me, Done: px.peers_min_done[px.me]}

  for i, peer := range px.peers {
    go func(i int, peer string) {
      var decide_reply DecideReply
      if i != px.me {
        ok := call(peer, "Paxos.Decide", &decide_args, &decide_reply)
        if !ok {
          // log.Printf("Paxos[%v] Decide ERR\n", i) //## DEBUG
        }
      } else {
        px.Decide(&decide_args, &decide_reply)
      }
    }(i, peer)
  }
}

func (px *Paxos) pick_n(seq int) int {
  hint := 0
  px.mu.Lock()
  ins, found := px.ins_memo[seq]
  px.mu.Unlock()
  if found {
    hint = ins.n_p
  }
  return px.cal_proposal_num(hint)
}

func (px *Paxos) cal_proposal_num(hint int) int {
  base := hint/px.total+1
  return base*px.total+px.me
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
	defer px.mu.Unlock()

  ins, found := px.ins_memo[args.Seq]
  if !found {
    reply.Status = OK
    reply.Na = 0
    reply.Va = nil
    px.ins_memo[args.Seq] = Instance{n_p: args.N, n_a: 0, v_a: nil, decided: false}
    // log.Printf("   Paxos[%v] Prepare seq[%v]: ok - not found\n", px.me, args.Seq) //## DEBUG
  } else {
    if ins.n_p < args.N {
      reply.Status = OK
      reply.Na = ins.n_a
      reply.Va = ins.v_a
      ins.n_p = args.N
      px.ins_memo[args.Seq] = ins
      // log.Printf("   Paxos[%v] Prepare seq[%v]: ok - higher n\n", px.me, args.Seq) //## DEBUG
    } else {
      reply.Status = REJECT
      reply.NpHint = ins.n_p
      // log.Printf("   Paxos[%v] Prepare seq[%v]: rej - %v >= %v\n", px.me, args.Seq, ins.n_p, args.N) //## DEBUG
    }
  }
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
	defer px.mu.Unlock()

  // log.Printf("   Paxos[%v] Accept seq[%v]\n", px.me, args.Seq) //## DEBUG

  ins, found := px.ins_memo[args.Seq]
  if !found {
    reply.Status = OK
    px.ins_memo[args.Seq] = Instance{n_p: args.N, n_a: args.N, v_a: args.V, decided: false}
  } else {
    if ins.n_p <= args.N {
      reply.Status = OK
      ins.n_p = args.N
      ins.n_a = args.N
      ins.v_a = args.V
      px.ins_memo[args.Seq] = ins
    } else {
      reply.Status = REJECT
      reply.NpHint = ins.n_p
    }
  }
  return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
	defer px.mu.Unlock()

  // log.Printf("   Paxos[%v] Decide seq[%v]\n", px.me, args.Seq) //## DEBUG

  ins, found := px.ins_memo[args.Seq]
  if !found {
    px.ins_memo[args.Seq] = Instance{n_p: 0, n_a: 0, v_a: args.V, decided: true}
  } else {
    ins.v_a = args.V
    ins.decided = true
    px.ins_memo[args.Seq] = ins
  }
	px.peers_min_done[args.Peer] = args.Done

  reply.Status = OK
  return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

  if px.peers_min_done[px.me] < seq {
    px.peers_min_done[px.me] = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
	px.mu.Lock()
  defer px.mu.Unlock()

	max_seq := -1
	for s, _ := range px.ins_memo {
		if max_seq < s {
			max_seq = s
		}
	}

	return max_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
	px.mu.Lock()
  defer px.mu.Unlock()

	min_seq := px.peers_min_done[0]
	for _, s := range px.peers_min_done {
		if min_seq > s {
			min_seq = s
		}
	}

	for s := 0; s < min_seq; s++ {
		ins, found := px.ins_memo[s]
		if found && ins.decided {
			delete(px.ins_memo, s)
		}
	}

	return min_seq+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	
	ins, found := px.ins_memo[seq]
	if found && ins.decided {
    // log.Printf("Paxos[%v] decided seq[%v] \n", px.me, seq) //## DEBUG
		return true, ins.v_a
	}
  // log.Printf("Paxos[%v] undecided seq[%v], current memo: %v \n", px.me, seq, px.ins_memo) //## DEBUG
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.total = len(peers)
  px.last_min = 0
  px.ins_memo = make(map[int]Instance)
  px.peers_min_done = make(map[int]int)
  for i := 0; i < px.total; i++ {
    px.peers_min_done[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
