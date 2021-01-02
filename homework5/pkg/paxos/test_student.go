package paxos

import (
	"coms4113/hw5/pkg/base"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		if s1.proposer.Phase == Propose {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.From() == s2.Address() && m.To() == s1.Address() {
					return true
				}
			}
		}
		return false
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		if s3.proposer.Phase == Propose {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.From() == s2.Address() && m.To() == s3.Address() {
					return true
				}
			}
		}
		return false
	}

	a2RejectP1Phase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		if s1.proposer.Phase == Accept {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.From() == s2.Address() && m.To() == s1.Address() {
					return true
				}
			}
		}
		return false
	}

	return []func(s *base.State) bool{p1PreparePhase, p3PreparePhase, a2RejectP1Phase}

}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	a2AcceptP3Phase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		if s3.proposer.Phase == Accept {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && resp.Ok && m.From() == s2.Address() && m.To() == s3.Address() {
					return true
				}
			}
		}
		return false
	}

	return []func(s *base.State) bool{a2AcceptP3Phase}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {

	p1PreparePhase1 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose && s3.proposer.Phase == "" && s1.proposer.ResponseCount == 1
	}

	p1PreparePhase2 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose && s3.proposer.Phase == "" && s1.proposer.ResponseCount == 2 && s1.proposer.SuccessCount == 2
	}

	p3PreparePhase1 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 1
	}

	p3PreparePhase2 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 2 && server.proposer.SuccessCount == 2
	}

	p1PreparePhase3 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s1"].(*Server)
		return server.proposer.Phase == Accept && server.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{p1PreparePhase1, p1PreparePhase2, p3PreparePhase1, p3PreparePhase2, p1PreparePhase3}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p1PreparePhase1 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		// s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.ResponseCount == 1
	}

	p1PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s1"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 2 && server.proposer.SuccessCount == 2
	}

	p3PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Accept && server.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{p1PreparePhase1, p1PreparePhase, p3PreparePhase}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	p3PreparePhase1 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 1
	}

	p3PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 2 && server.proposer.SuccessCount == 2
	}

	p1PreparePhase3 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s1"].(*Server)
		return server.proposer.Phase == Accept && server.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{p3PreparePhase1, p3PreparePhase, p1PreparePhase3}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	p1PreparePhase1 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose && s3.proposer.Phase == "" && s1.proposer.ResponseCount == 1
	}

	p1PreparePhase2 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose && s3.proposer.Phase == "" && s1.proposer.ResponseCount == 2 && s1.proposer.SuccessCount == 2
	}

	p3PreparePhase1 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 1
	}

	p3PreparePhase2 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Propose && server.proposer.ResponseCount == 2 && server.proposer.SuccessCount == 2
	}

	p1PreparePhase3 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s1"].(*Server)
		return server.proposer.Phase == Accept && server.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{p1PreparePhase1, p1PreparePhase2, p3PreparePhase1, p3PreparePhase2, p1PreparePhase3}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		server := s.Nodes()["s3"].(*Server)
		return server.proposer.Phase == Accept && server.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{p3PreparePhase}
}
