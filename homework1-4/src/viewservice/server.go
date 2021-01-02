
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  curr_view View
  acked bool
  now time.Time
  last_pings map[string]time.Time
  idle string
}

const grace_period = DeadPings * PingInterval

func (vs *ViewServer) replace_primary() {
	if vs.acked && vs.curr_view.Backup != "" {
		vs.update_view(vs.curr_view.Backup, vs.idle)
    vs.idle = ""
	}
}

func (vs *ViewServer) replace_backup() {
	if vs.acked && vs.idle != "" {
		vs.update_view(vs.curr_view.Primary, vs.idle)
    vs.idle = ""
	}
}

func (vs *ViewServer) update_view(primary string, backup string) {
	vs.curr_view.Viewnum += 1
	vs.curr_view.Primary = primary
	vs.curr_view.Backup = backup
	vs.acked = false
	vs.idle = ""
}

func (vs *ViewServer) check_server(server string) bool {
  return (server != "") && (vs.now.Sub(vs.last_pings[server]) < grace_period)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  // lock the vs
  vs.mu.Lock()
	defer vs.mu.Unlock()

  // get the server and the view number it pings
	server := args.Me
  view_num := args.Viewnum

	//  update this server's latest ping time
	vs.last_pings[server] = time.Now()

  switch server {
	case vs.curr_view.Primary:
		if view_num == vs.curr_view.Viewnum {
      // if the view number equals to the current view number, mark acknowleged flag as true
			vs.acked = true
		} else {
      // wrong view number - replace the primary
			vs.replace_primary()
		}
	case vs.curr_view.Backup:
    // view number is 0 (encontered a crash) - replace the backup
		if view_num == 0 {
			vs.replace_backup()
		}
	default:
    if vs.curr_view.Viewnum == 0 {
       // 1st time, so make whatever as primary
			vs.update_view(server, "")
		} else {
      // found a idle server
			vs.idle = server
		}
	}

  reply.View = vs.curr_view

  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.curr_view
  return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
	defer vs.mu.Unlock()

  vs.now = time.Now()

  // check and remove inactive server
  if !vs.check_server(vs.idle){
    vs.idle = ""
  }
  if !vs.check_server(vs.curr_view.Primary) {
    vs.replace_primary()
  }
  if !vs.check_server(vs.curr_view.Backup) {
    vs.replace_backup()
  }
}


//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.curr_view = View{Viewnum: 0, Primary: "", Backup: ""}
  vs.acked = false
	vs.last_pings = make(map[string]time.Time)
	vs.idle = ""

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
