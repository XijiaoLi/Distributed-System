package paxos

import (
	"coms4113/hw5/pkg/base"
	// "fmt"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) find_peer_id(addr base.Address) int {
	for i, peer := range(server.peers) {
		if peer == addr {
			return i
		}
	}

	return -1
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//implement it
	if !base.IsNil(server.agreedValue) {
    return []base.Node{server}
  }

	propose_request, ok := message.(*ProposeRequest)
	if ok {
		newNode := server.copy()
		propose_reply := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(propose_request.To(), propose_request.From()),
			SessionId: propose_request.SessionId,
		}

		if propose_request.N <= server.n_p {
			propose_reply.N_p = server.n_p
			propose_reply.Ok = false
		} else {
			newNode.n_p = propose_request.N
			propose_reply.V_a = server.v_a
			propose_reply.N_a = server.n_a
			propose_reply.Ok = true
		}

		newNode.SetSingleResponse(propose_reply)
		return []base.Node{newNode}
	}

	accept_request, ok := message.(*AcceptRequest)
	if ok {
		newNode := server.copy()

		accept_reply := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(accept_request.To(), accept_request.From()),
			SessionId: accept_request.SessionId,
		}

		if accept_request.N < server.n_p {
			accept_reply.N_p = server.n_p
			accept_reply.Ok = false
		} else {
			newNode.n_p = accept_request.N
			newNode.n_a = accept_request.N
			newNode.v_a = accept_request.V
			accept_reply.Ok = true
		}

		newNode.SetSingleResponse(accept_reply)
		return []base.Node{newNode}
	}

	// handler of the decide request
	decide_request, ok := message.(*DecideRequest)
	if ok {
		newNode := server.copy()
		newNode.agreedValue = decide_request.V
		return []base.Node{newNode}
	}

	propose_reply, ok := message.(*ProposeResponse)
	if ok {

		if server.proposer.Phase != Propose {
			return []base.Node{server}
		}

		peer_index := server.find_peer_id(propose_reply.From())
		if server.proposer.Responses[peer_index] {
			return []base.Node{server}
		}

		newNodes := make([]base.Node, 0, 4)
		newNode := server.copy()
		newNode.proposer.Responses[peer_index] = true
		newNode.proposer.ResponseCount ++

		if propose_reply.Ok {
			newNode.proposer.SuccessCount ++
			if newNode.proposer.N_a_max < propose_reply.N_a {
				newNode.proposer.N_a_max = propose_reply.N_a
				newNode.proposer.V = propose_reply.V_a
			}

			if newNode.proposer.SuccessCount > len(server.peers)/2 {
				newNode_acc := newNode.copy()
				newNode_acc.proposer.Phase = Accept
				newNode_acc.proposer.Responses = make([]bool, len(server.peers))
				newNode_acc.proposer.ResponseCount = 0
				newNode_acc.proposer.SuccessCount = 0

				msg := make([]base.Message, len(server.peers))
				from_me := server.peers[server.me]
				for i, to_peer := range server.peers {
					newMsg := &AcceptRequest {
						CoreMessage: base.MakeCoreMessage(from_me, to_peer),
						N: newNode_acc.proposer.N,
						V: newNode_acc.proposer.V,
						SessionId: newNode_acc.proposer.SessionId,
					}
					msg[i] = newMsg
				}
				newNode_acc.SetResponse(msg)
				newNodes = append(newNodes, newNode_acc)
			}
		} else {
			if newNode.n_p < propose_reply.N_p {
				newNode.n_p = propose_reply.N_p
			}
		}
		newNodes = append(newNodes, newNode)
		return newNodes
	}


	accept_reply, ok := message.(*AcceptResponse)
	if ok {
		if server.proposer.Phase != Accept {
			return []base.Node{server}
		}

		peer_index := server.find_peer_id(accept_reply.From())
		if server.proposer.Responses[peer_index] {
			return []base.Node{server}
		}

		newNodes := make([]base.Node, 0, 4)
		newNode := server.copy()
		newNode.proposer.Responses[peer_index] = true
		newNode.proposer.ResponseCount += 1

		if accept_reply.Ok {
			newNode.proposer.SuccessCount ++
			if newNode.proposer.SuccessCount > len(server.peers)/2 {
				newNode_dec := newNode.copy()
				newNode_dec.proposer.Phase = Decide
				newNode_dec.proposer.Responses = make([]bool, len(server.peers))
				newNode_dec.proposer.ResponseCount = 0
				newNode_dec.proposer.SuccessCount = 0

				msg := make([]base.Message, len(server.peers))
				from_me := server.peers[server.me]
				for i, to_peer := range server.peers {
					m := &DecideRequest {
						CoreMessage: base.MakeCoreMessage(from_me, to_peer),
						SessionId: server.proposer.SessionId,
						V: server.proposer.V,
					}
					msg[i] = m
				}
				newNode_dec.SetResponse(msg)
				newNodes = append(newNodes, newNode_dec)
			}
		} else {
			if newNode.n_p < accept_reply.N_p {
				newNode.n_p = accept_reply.N_p
			}
		}
		newNodes = append(newNodes, newNode)
		return newNodes
	}

	return []base.Node{server} // should not come here!
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	if base.IsNil(server.proposer.InitialValue) {
    return
  }
	//TODO: implement it
	msg := make([]base.Message, len(server.peers))

	server.proposer.N = server.n_p + 1
	server.proposer.Phase = Propose
	server.proposer.N_a_max = 0
	server.proposer.V = server.proposer.InitialValue
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.SessionId++

	// from := strconv.Itoa(server.me)

	for i, p := range server.peers {
		m := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.peers[server.me], p),
			N: server.proposer.N,
			SessionId: server.proposer.SessionId,
		}
		msg[i] = m
	}
	// fmt.Printf("msg: %v\n", msg)

	server.SetResponse(msg)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
