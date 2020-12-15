package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
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
	//TODO: implement it
	if server.agreedValue != nil {
		return []base.Node{server}
	}
	// request handler ------------------------------------------------------------------------

	pro_req, ok := message.(*ProposeRequest)
	if ok {
		newNode := server.copy()
		pro_resp := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(pro_req.To(), pro_req.From()),
			SessionId: pro_req.SessionId,
		}

		if pro_req.N <= server.n_p {
			pro_resp.Ok = false
			pro_resp.N_p = server.n_p
		} else {
			newNode.n_p = pro_req.N
			pro_resp.Ok = true
			pro_resp.N_a = server.n_a
			pro_resp.V_a = server.v_a
		}

		newNode.SetSingleResponse(pro_resp)
		return []base.Node{newNode}
	}

	acc_req, ok := message.(*AcceptRequest)
	if ok {
		newNode := server.copy()
		acc_resp := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(acc_req.To(), acc_req.From()),
			SessionId: acc_req.SessionId,
		}

		if acc_req.N < server.n_p {
			acc_resp.Ok = false
			acc_resp.N_p = server.n_p
		} else {
			newNode.n_p = acc_req.N
			newNode.n_a = acc_req.N
			newNode.v_a = acc_req.V
			acc_resp.Ok = true
		}

		newNode.SetSingleResponse(acc_resp)
		return []base.Node{newNode}
	}

	dec_req, ok := message.(*DecideRequest)
	if ok {
		newNode := server.copy()
		newNode.agreedValue = dec_req.V
		return []base.Node{newNode}
	}

	// response handler ------------------------------------------------------------------------

	// case 4: receive a ProposeResponse
	pro_resp, ok := message.(*ProposeResponse)
	if ok {
		// 4.1 ignore the message: wrong session id / server not at the Propose Phase / repeated response
		if pro_resp.SessionId != server.proposer.SessionId || server.proposer.Phase != Propose {
			return []base.Node{server}
		}

		peer_id := server.find_peer_id(pro_resp.From())
		fmt.Printf("peer id: %v\n", peer_id)
		if peer_id  == -1 || server.proposer.Responses[peer_id] {
			return []base.Node{server}
		}

		newNodes := make([]base.Node, 0, 4)
		newNode := server.copy()
		newNode.proposer.ResponseCount++
		newNode.proposer.Responses[peer_id] = true

		if pro_resp.Ok {
			// 4.2 handle the message: prepare is ok

			// 4.2.1 server remains at Propose Phase
			newNode.proposer.SuccessCount++
			if newNode.proposer.N_a_max < pro_resp.N_a {
				newNode.proposer.N_a_max = pro_resp.N_a
				newNode.proposer.V = pro_resp.V_a
			}

		} else {
			// 4.3 handle the message: prepare declined
			if server.n_p < pro_resp.N_p {
				newNode.n_p = pro_resp.N_p
			}
		}

		if newNode.proposer.SuccessCount > len(server.peers)/2 {
			// 4.2.2 server moves to at Accept Phase
			newNode2 := newNode.copy()
			newNode2.proposer.Phase = Accept
			newNode2.proposer.Responses = make([]bool, len(server.peers))
			newNode2.proposer.ResponseCount = 0
			newNode2.proposer.SuccessCount = 0

			msg := make([]base.Message, len(server.peers))
			for i, p := range server.peers {
				m := &AcceptRequest{
					CoreMessage: base.MakeCoreMessage(server.peers[server.me], p),
					N: server.proposer.N,
					V: server.proposer.V,
					SessionId: server.proposer.SessionId,
				}
				msg[i] = m
			}

			newNode2.SetResponse(msg)
			newNodes = append(newNodes, newNode2) // 4.2.2
		}

		if newNode.proposer.ResponseCount == len(server.peers) {
			newNode.StartPropose()
		}
		newNodes = append(newNodes, newNode)

		fmt.Printf("%v\n", newNodes)

		return newNodes
	}

	// --------------

	acc_resp, ok := message.(*AcceptResponse)
	if ok {
		// 5.1 ignore the message: wrong session id / server not at the Accept Phase / repeated response
		if acc_resp.SessionId != server.proposer.SessionId || server.proposer.Phase != Accept { //
			return []base.Node{server}
		}

		peer_id := server.find_peer_id(acc_resp.From())
		if peer_id  == -1 || server.proposer.Responses[peer_id] {
			return []base.Node{server}
		}

		newNodes := make([]base.Node, 0, 4)
		newNode := server.copy()
		newNode.proposer.ResponseCount++
		newNode.proposer.Responses[peer_id] = true

		if acc_resp.Ok {
			// 5.2 handle the message: accept is ok

			// 5.2.1 server remains at Accept Phase
			newNode.proposer.SuccessCount++
		} else {
			// 5.3 handle the message: prepare declined
			if server.n_p < acc_resp.N_p {
				newNode.n_p = acc_resp.N_p
			}
		}

		if newNode.proposer.SuccessCount > len(server.peers)/2 {
			// 5.2.2 server moves to at Decide Phase
			newNode2 := newNode.copy()
			newNode2.proposer.Phase = Decide
			newNode2.proposer.Responses = make([]bool, len(server.peers))
			newNode2.proposer.ResponseCount = 0
			newNode2.proposer.SuccessCount = 0
			newNode2.agreedValue = server.proposer.V

			msg := make([]base.Message, len(server.peers))
			for i, p := range server.peers {
				m := &DecideRequest{
					CoreMessage: base.MakeCoreMessage(server.peers[server.me], p),
					V: server.proposer.V,
					SessionId: server.proposer.SessionId,
				}
				msg[i] = m
			}

			newNode2.SetResponse(msg)
			newNodes = append(newNodes, newNode2) // 5.2.2
		}

		if newNode.proposer.ResponseCount == len(server.peers) {
			newNode.StartPropose()
		}
		newNodes = append(newNodes, newNode)

		return newNodes
	}

	fmt.Printf("\n\nsomething is wrong!!!\n\n")
	return []base.Node{server}
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
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
