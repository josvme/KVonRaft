package main

import (
	"testing"
)

func TestAppendEntries_TermCheck(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	s.PersistentState.currentTerm = 2

	req := AppendEntriesRequest{
		Term: 1,
	}
	resp := s.AppendEntries(req)
	if resp.Success {
		t.Error("Expected failure for lower term")
	}
	if resp.Term != 2 {
		t.Errorf("Expected term 2, got %d", resp.Term)
	}
}

func TestRequestVote_Success(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	s.PersistentState.currentTerm = 1
	// Last log index is 0, term 0 (dummy log)

	req := RequestVoteRequest{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := s.RequestVote(req)
	if !resp.VoteGranted {
		t.Error("Expected vote to be granted")
	}
	if s.PersistentState.votedFor != 2 {
		t.Errorf("Expected votedFor 2, got %d", s.PersistentState.votedFor)
	}
}

func TestRequestVote_OldTerm(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	s.PersistentState.currentTerm = 2

	req := RequestVoteRequest{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := s.RequestVote(req)
	if resp.VoteGranted {
		t.Error("Expected vote NOT to be granted for old term")
	}
}

func TestAppendEntries_Conflict(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	s.PersistentState.currentTerm = 1
	s.PersistentState.log[1] = Log{Term: 1, Command: "key1", Content: "val1"}
	s.PersistentState.log[2] = Log{Term: 1, Command: "key2", Content: "val2"}

	req := AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []Log{
			{Term: 1, Command: "key2", Content: "val2-new"},
		},
	}
	resp := s.AppendEntries(req)
	if !resp.Success {
		t.Error("Expected success")
	}
	if s.PersistentState.log[2].Content != "val2-new" {
		t.Errorf("Expected val2-new, got %s", s.PersistentState.log[2].Content)
	}
}

func TestApplyLogs(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	s.PersistentState.log[1] = Log{Term: 1, Command: "k1", Content: "v1"}
	s.VolatileState.commitIndex = 1

	s.applyLogsToStateMachine()

	if s.state["k1"] != "v1" {
		t.Errorf("Expected k1=v1, got %v", s.state["k1"])
	}
	if s.VolatileState.lastApplied != 2 {
		t.Errorf("Expected lastApplied 2, got %d", s.VolatileState.lastApplied)
	}
}
