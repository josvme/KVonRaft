package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KV struct {
	Key   string
	Value string
}

type Log struct {
	Command string
	Term    int
	Content string
}
type PersistentState struct {
	currentTerm int
	votedFor    int
	log         map[int]Log
}

type VolatileState struct {
	commitIndex int
	lastApplied int
}

type VolatileLeaderState struct {
	nextIndex  map[int]int
	matchIndex map[int]int
}

type Server struct {
	PeristentState      PersistentState
	VolatileState       VolatileState
	VolatileLeaderState VolatileLeaderState
	nodeId              int
	mutex               sync.Mutex
	role                string
	state               map[string]string
	stateMachine        func(state map[string]string, k string, v string) map[string]string
	neighbourNodes      []int
	lastRequestTime     time.Time
	lastVoteTime        time.Time
	logger              *slog.Logger
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type RequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

func NewServer(nodeId int, neighbours []int) *Server {
	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})
	logger := slog.New(jsonHandler)
	l := Log{
		Command: "dummy",
		Term:    0,
		Content: "dummy",
	}
	return &Server{
		PeristentState:      PersistentState{votedFor: -1, currentTerm: 0, log: map[int]Log{0: l}},
		VolatileState:       VolatileState{commitIndex: 0, lastApplied: 0},
		VolatileLeaderState: VolatileLeaderState{nextIndex: make(map[int]int), matchIndex: make(map[int]int)},
		mutex:               sync.Mutex{},
		nodeId:              nodeId,
		neighbourNodes:      neighbours,
		role:                "follower",
		state:               make(map[string]string),
		stateMachine: func(state map[string]string, k string, v string) map[string]string {
			state[k] = v
			return state
		},
		logger: logger,
	}
}

func (s *Server) appendEntriesEndpoint(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := s.AppendEntries(req)
	json.NewEncoder(w).Encode(&resp)
}

func (s *Server) requestVoteEndpoint(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := s.RequestVote(req)
	json.NewEncoder(w).Encode(&resp)
}

func (s *Server) getValueEndpoint(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	s.mutex.Lock()
	v, ok := s.state[key]
	role := s.role
	s.mutex.Unlock()

	if !ok {
		http.Error(w, "Key not found", http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"Success": "true", "role": role, "value": v})
}

func (s *Server) fullValueEndpoint(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"Success": "true", "role": s.role, "value": fmt.Sprint(s.state), "entries": fmt.Sprint(s.PeristentState.log), "volatile": fmt.Sprint(s.VolatileState)})
}

func (s *Server) dieEndpoint(w http.ResponseWriter, r *http.Request) {
	// This wont be sent anyway
	s.mutex.Lock()
	role := s.role
	s.mutex.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "dying", "role": role})
	os.Exit(0)
}

func (s *Server) leaderEndpoint(w http.ResponseWriter, r *http.Request) {
	type Reply struct {
		Leader string
		NodeId int
	}
	s.mutex.Lock()
	role := s.role
	s.mutex.Unlock()
	if role == "leader" {
		json.NewEncoder(w).Encode(Reply{
			Leader: "true",
			NodeId: s.nodeId,
		})
	} else {
		json.NewEncoder(w).Encode(Reply{
			Leader: "false",
			NodeId: s.nodeId,
		})
	}
}

func (s *Server) insertValueEndpoint(w http.ResponseWriter, r *http.Request) {
	var req KV
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mutex.Lock()
	if s.role != "leader" {
		s.mutex.Unlock()
		http.Error(w, "Please send to leader node", http.StatusBadRequest)
		return
	}

	// Add to local log the request
	lastIndex := s.getLastLogIndexLocked()
	s.PeristentState.log[lastIndex+1] = Log{Term: s.PeristentState.currentTerm, Command: req.Key, Content: req.Value}

	requests := make(map[int][]byte)

	// Send append requests based on nextIndex and matchIndex
	for _, n := range s.neighbourNodes {
		prevLogIndex := s.VolatileLeaderState.nextIndex[n] - 1
		prevLogTerm := s.PeristentState.log[prevLogIndex].Term
		entries := make([]Log, 0)

		for i := prevLogIndex + 1; i <= s.getLastLogIndexLocked(); i++ {
			entries = append(entries, s.PeristentState.log[i])
		}

		appendEntriesRequest := AppendEntriesRequest{
			Term:         s.PeristentState.currentTerm,
			LeaderId:     s.nodeId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: s.VolatileState.commitIndex,
		}

		j, err := json.Marshal(appendEntriesRequest)
		if err != nil {
			// this shouldn't happen
			panic(err)
		}
		requests[n] = j
	}
	s.mutex.Unlock()

	successCount, rejectRequest := s.sendToFollowers(requests)

	if rejectRequest {
		http.Error(w, "Please send to leader node", http.StatusBadRequest)
		return
	}

	// It is a Success, append to our log
	s.mutex.Lock()
	if successCount >= len(s.neighbourNodes)/2 {
		s.PeristentState.log[s.VolatileState.commitIndex+1] = Log{
			Command: req.Key,
			Term:    s.PeristentState.currentTerm,
			Content: req.Value,
		}
		s.VolatileState.commitIndex++
		s.applySnapshotLocked()
		commitIndex := s.VolatileState.commitIndex
		s.mutex.Unlock()
		s.logger.Info("Applied commit index: ", "commitIndex", commitIndex)
		json.NewEncoder(w).Encode(map[string]string{"Success": "true"})
		return
	}
	s.mutex.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"Success": "false", "reason": "majority not reached or timed out"})
}

func (s *Server) sendToFollowers(j map[int][]byte) (int, bool) {
	outputs := make(map[int]AppendEntriesResponse)
	for _, n := range s.neighbourNodes {
		resp, err := http.Post("http://localhost:"+strconv.Itoa(8000+n)+"/appendEntries", "application/json", bytes.NewReader(j[n]))
		if err != nil {
			// move to next node
			continue
		}

		var appendEntriesResponse AppendEntriesResponse
		err = json.NewDecoder(resp.Body).Decode(&appendEntriesResponse)
		if err != nil {
			// move to next node
			continue
		}
		outputs[n] = appendEntriesResponse
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	successCount := 0
	rejectRequest := false
	// Handle cases where followers gave false, because they are far behind
	for nodeId, v := range outputs {
		if v.Success {
			// ideally index updates should be based on contents of j.
			// But even if it is wrong, we have mechanism to fix this builtin to the raft protocol
			lastLogIndex := s.getLastLogIndexLocked()
			s.VolatileLeaderState.nextIndex[nodeId] = lastLogIndex + 1
			s.VolatileLeaderState.matchIndex[nodeId] = lastLogIndex
			successCount++
		}
		if v.Term > s.PeristentState.currentTerm {
			// There is someone with a more updated Term
			s.role = "follower"
			s.PeristentState.currentTerm = v.Term
			rejectRequest = true
			break
		}
		if !v.Success {
			// Logs are behind
			s.VolatileLeaderState.nextIndex[nodeId] -= 1
			s.VolatileLeaderState.matchIndex[nodeId] -= 1
		}
	}
	return successCount, rejectRequest
}

func (s *Server) GetLastLogIndex() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.getLastLogIndexLocked()
}

func (s *Server) getLastLogIndexLocked() int {
	keys := slices.Sorted(maps.Keys(s.PeristentState.log))
	if len(keys) == 0 {
		return 0
	}
	// Get last element index
	return keys[len(keys)-1]
}
func (s *Server) AppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Reset voted for if successful
	s.PeristentState.votedFor = -1
	s.lastRequestTime = time.Now()
	// Rule for all servers
	if request.Term > s.PeristentState.currentTerm {
		s.PeristentState.currentTerm = request.Term
		s.role = "follower"
	}
	out := AppendEntriesResponse{}
	if request.Term < s.PeristentState.currentTerm {
		out = AppendEntriesResponse{Term: s.PeristentState.currentTerm, Success: false}
		return out
	}

	v, ok := s.PeristentState.log[request.PrevLogIndex]
	if !ok {
		s.logger.Info("AppendEntries failed, previous log not found", "request", request, "nodeId", s.nodeId)
		return AppendEntriesResponse{Term: s.PeristentState.currentTerm, Success: false}
	}

	// term doesn't match
	if v.Term != request.PrevLogTerm {
		s.logger.Info("AppendEntries failed", "request", request, "out", out, "nodeId", s.nodeId)
		return AppendEntriesResponse{Term: s.PeristentState.currentTerm, Success: false}
	}

	// delete the existing entry and all following Entries in the request
	keys := slices.Sorted(maps.Keys(s.PeristentState.log))
	for _, key := range keys {
		if key > request.PrevLogIndex {
			delete(s.PeristentState.log, key)
		}
	}
	// Append all new Entries
	for i, entry := range request.Entries {
		s.PeristentState.log[request.PrevLogIndex+i+1] = entry
	}

	if request.LeaderCommit > s.VolatileState.commitIndex {
		lastLogIndex := s.getLastLogIndexLocked()
		s.VolatileState.commitIndex = min(request.LeaderCommit, lastLogIndex)
	}

	// Everything seems fine
	out = AppendEntriesResponse{Term: s.PeristentState.currentTerm, Success: true}
	if len(request.Entries) > 0 {
		s.logger.Info("AppendEntries successful", "request", request, "out", out, "nodeId", s.nodeId)
	} else {
		s.logger.Info("Heartbeat received", "request", request, "out", out, "nodeId", s.nodeId)
	}
	return out
}

func (s *Server) RequestVote(request RequestVoteRequest) RequestVoteResponse {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// To check if we should become a candidate
	s.lastRequestTime = time.Now()

	// Reset votedFor after election timeout time
	if time.Since(s.lastVoteTime) > time.Second*1 {
		s.PeristentState.votedFor = -1
	}
	// Rule for all servers
	if request.Term > s.PeristentState.currentTerm {
		s.PeristentState.currentTerm = request.Term
		s.role = "follower"
	}
	out := RequestVoteResponse{}
	// If we are more updated than the candidate
	if request.Term < s.PeristentState.currentTerm {
		out = RequestVoteResponse{Term: s.PeristentState.currentTerm, VoteGranted: false}
		s.logger.Info("RequestVote Overruled", "request", request, "out", out)
		return out
	}

	// We haven't voted for anyone
	if s.PeristentState.votedFor == -1 {
		last := s.getLastLogIndexLocked()
		// The candidate should have the last log entry atleast as up to date as us
		if request.LastLogIndex >= last && (request.LastLogTerm >= s.PeristentState.log[last].Term) {
			out = RequestVoteResponse{Term: s.PeristentState.currentTerm, VoteGranted: true}
		} else {
			out = RequestVoteResponse{Term: s.PeristentState.currentTerm, VoteGranted: false}
		}

		if out.VoteGranted {
			s.PeristentState.votedFor = request.CandidateId
			s.lastVoteTime = time.Now()
			s.logger.Info("RequestVote voted", "request", request, "out", out, "nodeId", s.nodeId)
		}
		return out
	} else {
		out = RequestVoteResponse{Term: s.PeristentState.currentTerm, VoteGranted: false}
		s.logger.Info("RequestVote already voted", "request", request, "out", out, "nodeId", s.nodeId)
		return out
	}
}

func (s *Server) LeaderHeartbeat() {
	for {
		s.mutex.Lock()
		role := s.role
		s.mutex.Unlock()
		if role == "leader" {
			s.sendPeriodicHeartBeat()
		}
		time.Sleep(time.Millisecond * 400)
	}
}

func (s *Server) sendPeriodicHeartBeat() {
	s.mutex.Lock()
	if s.role != "leader" {
		s.mutex.Unlock()
		return
	}

	requests := make(map[int][]byte)

	// Send append requests based on nextIndex and matchIndex
	for _, n := range s.neighbourNodes {
		prevLogIndex := s.VolatileLeaderState.nextIndex[n] - 1
		prevLogTerm := s.PeristentState.log[prevLogIndex].Term
		entries := make([]Log, 0)

		for i := prevLogIndex + 1; i <= s.getLastLogIndexLocked(); i++ {
			entries = append(entries, s.PeristentState.log[i])
		}

		appendEntriesRequest := AppendEntriesRequest{
			Term:         s.PeristentState.currentTerm,
			LeaderId:     s.nodeId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: s.VolatileState.commitIndex,
		}

		j, err := json.Marshal(appendEntriesRequest)
		if err != nil {
			// this shouldn't happen
			panic(err)
		}
		requests[n] = j
	}
	s.mutex.Unlock()

	_, _ = s.sendToFollowers(requests)
}

func (s *Server) sendHeartBeatEmpty() {
	s.mutex.Lock()
	lastLogIndex := s.getLastLogIndexLocked()

	heartBeatRequest := AppendEntriesRequest{
		Term:         s.PeristentState.currentTerm,
		LeaderId:     s.nodeId,
		PrevLogIndex: lastLogIndex,
		Entries:      make([]Log, 0),
		LeaderCommit: s.VolatileState.commitIndex,
	}
	j, err := json.Marshal(heartBeatRequest)
	if err != nil {
		s.mutex.Unlock()
		panic(err)
	}
	neighbourNodes := make([]int, len(s.neighbourNodes))
	copy(neighbourNodes, s.neighbourNodes)
	s.mutex.Unlock()

	for _, n := range neighbourNodes {
		http.Post("http://localhost:"+strconv.Itoa(8000+n)+"/appendEntries", "application/json", bytes.NewReader(j))
	}
	s.logger.Info("Heartbeat send", "request", heartBeatRequest)
}

func (s *Server) startElection() {
	s.mutex.Lock()
	lastLogIndex := s.getLastLogIndexLocked()
	lastLogTerm := s.PeristentState.log[lastLogIndex].Term

	// Increment Term and start election
	s.PeristentState.currentTerm++
	requestVote := RequestVoteRequest{
		Term:         s.PeristentState.currentTerm,
		CandidateId:  s.nodeId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	s.logger.Info("Election start", "requestVote", requestVote, "nodeId", s.nodeId)
	j, err := json.Marshal(requestVote)
	if err != nil {
		s.logger.Error("Starting election failed with Json Marshal", "requestVote", requestVote, "err", err, "nodeId", s.nodeId)
	}

	s.PeristentState.votedFor = s.nodeId
	neighbourNodes := make([]int, len(s.neighbourNodes))
	copy(neighbourNodes, s.neighbourNodes)
	s.mutex.Unlock()

	outputs := make(map[int]RequestVoteResponse)
	for _, n := range neighbourNodes {
		resp, err := http.Post("http://localhost:"+strconv.Itoa(8000+n)+"/requestVote", "application/json", bytes.NewReader(j))
		if err != nil {
			s.logger.Error(err.Error(), "requestVote", requestVote, "nodeId", s.nodeId)
			continue
		}

		var requestVoteResponse RequestVoteResponse
		err = json.NewDecoder(resp.Body).Decode(&requestVoteResponse)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		outputs[n] = requestVoteResponse
	}

	s.mutex.Lock()
	successCount := 0
	rejectRequest := false
	for _, v := range outputs {
		if v.VoteGranted {
			successCount++
		}
		if v.Term > s.PeristentState.currentTerm {
			// There is someone with a more updated Term
			s.role = "follower"
			s.PeristentState.currentTerm = v.Term
			rejectRequest = true
			break
		}
	}

	if rejectRequest {
		s.mutex.Unlock()
		s.logger.Info("There is another candidate with higher Term", "requestVote", requestVote, "nodeId", s.nodeId)
		return
	}

	// We vote for ourselves
	if successCount >= len(s.neighbourNodes)/2 {
		s.logger.Info("New leader elected", "requestVote", requestVote, "nodeId", s.nodeId, "pid", os.Getpid())
		s.role = "leader"
		s.mutex.Unlock()
		s.sendHeartBeatEmpty()
		s.mutex.Lock()
		// Initialize the nextIndex and matchIndex for each follower
		s.VolatileLeaderState.nextIndex = make(map[int]int)
		s.VolatileLeaderState.matchIndex = make(map[int]int)
		for _, n := range s.neighbourNodes {
			s.VolatileLeaderState.nextIndex[n] = s.getLastLogIndexLocked() + 1
			s.VolatileLeaderState.matchIndex[n] = 0
		}
		s.mutex.Unlock()
		return
	}
	// Failed
	s.logger.Info("Election failed", "requestVote", requestVote, "nodeId", s.nodeId)
	s.role = "follower"
	s.mutex.Unlock()
}

func (s *Server) CandidateChecker() {
	for {
		s.mutex.Lock()
		if s.role == "follower" {
			if (time.Since(s.lastRequestTime) > 1*time.Second) && (time.Since(s.lastVoteTime) > 1*time.Second) {
				s.role = "candidate"
				s.lastVoteTime = time.Now()
				s.mutex.Unlock()
				s.startElection()
			} else {
				s.mutex.Unlock()
			}
		} else {
			s.mutex.Unlock()
		}
		// Jitter time
		time.Sleep(time.Duration(rand.IntN(100)) * time.Millisecond)
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *Server) SnapshotChecker() {
	for {
		// Apply changes to state machine
		s.ApplySnapshot()
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Server) ApplySnapshot() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.applySnapshotLocked()
}

func (s *Server) applySnapshotLocked() {
	// check if we can update the commit index
	keys := slices.Sorted(maps.Values(s.VolatileLeaderState.matchIndex))
	if len(keys) > 0 {
		commitOnAll := keys[len(keys)-1]

		// We need a no-op from leader to advance the commit index. We leave it up to clients
		if s.PeristentState.log[commitOnAll].Term == s.PeristentState.currentTerm {
			s.VolatileState.commitIndex = commitOnAll
		}
	}

	for s.VolatileState.commitIndex >= s.VolatileState.lastApplied {
		s.state = s.stateMachine(s.state, s.PeristentState.log[s.VolatileState.lastApplied].Command, s.PeristentState.log[s.VolatileState.lastApplied].Content)
		s.logger.Info("State updated", "state", s.state)
		s.VolatileState.lastApplied++
	}
}

func main() {
	nodeId, err := strconv.Atoi(os.Getenv("NODE_ID"))
	if err != nil {
		fmt.Println("node id must be an integer")
	}
	neighbors := make([]int, 0)
	for _, v := range strings.Split(os.Getenv("NEIGHBOR_NODE_IDS"), ",") {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Println("neighbour nodes must be an integers")
		}
		neighbors = append(neighbors, n)
	}
	server := NewServer(nodeId, neighbors)
	http.HandleFunc("/appendEntries", server.appendEntriesEndpoint)
	http.HandleFunc("/requestVote", server.requestVoteEndpoint)
	http.HandleFunc("/insert", server.insertValueEndpoint)
	http.HandleFunc("/get", server.getValueEndpoint)
	http.HandleFunc("/full", server.fullValueEndpoint)
	http.HandleFunc("/leader", server.leaderEndpoint)
	http.HandleFunc("/die", server.dieEndpoint)
	go server.CandidateChecker()
	go server.LeaderHeartbeat()
	go server.SnapshotChecker()
	port := 8000
	http.ListenAndServe(":"+strconv.Itoa(port+nodeId), nil)
}
