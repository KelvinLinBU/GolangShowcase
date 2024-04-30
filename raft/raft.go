package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	helperindex  int
	helperterm   int
}

const (
	Follower      = 0  //follower will be represented by 0
	Candidate     = 1  //candidate will be represented by 1
	Leader        = 2  //leader will be represented by 2
	Has_Not_Voted = -1 //if server has not voted, this value is -1
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // This peer's index into peers[]
	dead        int32               // Set by Kill()
	channelHelp chan ApplyMsg
	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int //latest term server (initialized to 0 on first boot, and increases monotonically
	votedFor    int //Candidiate id that received vote in current term (or null if none)
	//log entries; each entry contains command for state machine and
	//term when entry was received by leader. Take pointers to entry structs from config.go and store it
	log []LogEntry
	//Volatile State on All Servers
	commitIndex int //This is the index of the highest log entry known to be committed (initalized to 0 and increases monotonically)
	lastApplied int //Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	//Volatile State on Leaders
	nextIndex  []int //For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//others
	state               int           //int representing what state
	applyCH             chan ApplyMsg //set the applyCH struct
	election_timeout    *time.Timer   //timer for the election
	timer_for_heartbeat *time.Timer   //timer for sending out heartbeat RPCs
	killedchannel       chan struct{} //channel for ending
	APPLYCHANNEL        chan struct{} //channel to tell the applych.

	indexhelp int //vars to help implementation

}

type LogEntry struct { //LogEntry Struct, based after the config.go implementation
	Term    int         //the term in which this logEntry was created
	Command interface{} //the command
}

func (rf *Raft) ElectionTimeoutReset() { //helper function to reset the election timer
	rf.election_timeout.Stop()
	rf.election_timeout.Reset(time.Millisecond*150 + (time.Duration(rand.Int63()) % time.Millisecond * 150))

	//rf.persist()

}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //no issues with the getState implementation
	var term int
	var isleader bool
	// Your code here (4A).
	rf.mu.Lock() //lock the variables
	term = rf.currentTerm

	if rf.state == Leader { //if the state of the server is "Leader" then return true
		isleader = true
	} else {
		isleader = false //if the state of the server is not "Leader"
	}
	rf.mu.Unlock()
	return term, isleader //return the currentTerm and whether or not it is the leader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm) //encode the currentterm

	e.Encode(rf.votedFor)  //encode the votedFor
	e.Encode(rf.log)       //encode the log
	e.Encode(rf.indexhelp) //encode the

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var logs []LogEntry
	var indexhelp int

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&indexhelp) != nil {
		panic("not good")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.indexhelp = indexhelp
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct { //this is the AppendEntries Argument for RPC
	Term         int        //leader's term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat, may send more than one for efficiency)
	LeaderCommit int        //Leader's commitIndex
}

type AppendEntriesReply struct { //this is the appendEntriesReply for RPC
	Term     int  //currentTerm for leader to update itself
	Success  bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	LogTerm  int
	LogIndex int

	Abandon bool
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) { //the Handler for appendEntries basically the heartbeat according to the paper
	rf.mu.Lock()         //lock because accessing rf
	defer rf.mu.Unlock() //unlock after
	reply.Term = rf.currentTerm
	reply.Success = true            //set it to true for now to be changed later
	if rf.currentTerm < args.Term { // if the current term is less than the other node's term
		rf.state = Follower //recognize the other as leader and revert to follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { //added sendAppendEntries
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) //based after sendvoterequest
	return ok
}

func (rf *Raft) lastLogIndex() int {
	return rf.indexhelp + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int //candidate's term
	CandidiateId int //candidate requesting the vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  //currentTerm for candidate to update itself
	VoteGranted bool //true means candidate received the vote, otherwise they did not
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Acquire lock at the beginning to ensure thread safety.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize the reply.
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If the candidate's term is less than the current term, ignore the request.
	if args.Term < rf.currentTerm {
		return
	}

	// Adjust current term and reset votedFor if the candidate's term is newer.
	if args.Term > rf.currentTerm {

		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	// Conditions for granting vote:
	// 1. rf.votedFor is either -1 (indicating no vote yet) or the candidate's ID (indicating a re-vote for the same candidate).
	// 2. The candidate's log is at least as up-to-date as receiver's log.
	lastLogTerm := rf.lastLogTerm()
	lastLogIndex := rf.lastLogIndex()
	isLogUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidiateId) && isLogUpToDate {
		// Grant vote.
		reply.VoteGranted = true
		rf.votedFor = args.CandidiateId
		rf.state = Follower
		rf.ElectionTimeoutReset() // Reset the election timeout upon granting the vote.
		rf.persist()
	} else {
		// If not granted, ensure persistent state is up-to-date.
		rf.persist()
	}
}

func (rf *Raft) SendFilledAppendEntriesToAll() {
	for peer := range rf.peers { //for all peers
		if rf.me == peer { //if this is myself skip
			continue
		} else {
			go rf.AppendEntryGoroutine(peer) //else send the append entry heartbeat
		}
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
	}
	lastIndex := rf.lastLogIndex()
	index = lastIndex + 1
	if isLeader == true {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()

		rf.SendFilledAppendEntriesToAll()
	}

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.election_timeout.C: // Election timeout has occurred
			go func() {
				rf.mu.Lock()
				// Check if already a leader and exit if so
				if rf.state == Leader {
					rf.mu.Unlock()
					return
				}

				// Transition to candidate state and initialize election parameters
				rf.state = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.mu.Unlock()
				rf.ElectionTimeoutReset()
				rf.mu.Lock()
				rf.persist()
				rf.mu.Unlock()

				// Prepare RequestVoteArgs with the current snapshot of the index and term
				lastLogIndex := rf.lastLogIndex()
				lastLogTerm := rf.lastLogTerm()

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidiateId: rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}

				grantedVotes := 1                           // Vote for self
				totalVotes := 1                             // Including self
				votesCh := make(chan bool, len(rf.peers)-1) // Channel for votes

				// Send vote requests to all peers except self
				for i := range rf.peers {
					if i == rf.me {
						continue
					}

					go func(i int) {
						reply := RequestVoteReply{}

						if rf.sendRequestVote(i, &args, &reply) {
							votesCh <- reply.VoteGranted
						} else {
							votesCh <- false // Assume failure to get vote as a 'no'
						}

						rf.mu.Lock()

						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.persist()
							rf.ElectionTimeoutReset()
							rf.mu.Unlock()
							return // Exit early if a newer term is discovered
						}
						rf.mu.Unlock()
					}(i)
				}

				// Count votes
				for range rf.peers {
					voteGranted := <-votesCh
					totalVotes++
					if voteGranted {
						grantedVotes++
					}

					rf.mu.Lock()
					if totalVotes == len(rf.peers)-1 {
						// All votes are counted
						if grantedVotes > len(rf.peers)/2 && rf.state == Candidate {
							rf.state = Leader
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := range rf.peers {
								rf.nextIndex[i] = lastLogIndex + 1
								if i == rf.me {
									rf.matchIndex[i] = lastLogIndex
								}
							}
							rf.mu.Unlock()
							return // Leader elected
						}
						rf.mu.Unlock()
						break // Exit loop since all responses are received
					}
					rf.mu.Unlock()
				}
			}()

		case <-rf.killedchannel: // Check if Raft instance is killed
			return // Exit the ticker loop
		}
	}
}

func (rf *Raft) tellapplychannel() {
	for {
		select {
		case <-rf.APPLYCHANNEL:
			rf.processApplyMessages()
		case msg := <-rf.channelHelp:
			rf.processHelperMessage(msg)
		case <-rf.killedchannel:
			return
		}
	}
}

func (rf *Raft) processHelperMessage(msg ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.indexhelp >= msg.helperindex || rf.lastApplied >= msg.helperindex {
		return // No action if the helper index is not ahead of current indices
	}

	// Calculate the start index for adjustment
	start := msg.helperindex - rf.indexhelp
	if start >= len(rf.log) {
		// Completely reset the log if the helper index is out of current bounds
		rf.log = []LogEntry{{Term: msg.helperterm}}
	} else {
		// Adjust the log by truncating or shifting down as necessary
		rf.log = append([]LogEntry{{Term: msg.helperterm}}, rf.log[start:]...)
	}

	// Adjust index helpers according to the new settings
	rf.indexhelp = msg.helperindex
	rf.lastApplied = rf.indexhelp
}

func (rf *Raft) processApplyMessages() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.commitIndex {
		msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if i-rf.indexhelp >= len(rf.log) {
				// Ensure we do not go out of bounds
				break
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.indexhelp].Command,
				CommandIndex: i,
			}
			msgs = append(msgs, msg)
		}
		// Update lastApplied if messages were created
		if len(msgs) > 0 {
			rf.lastApplied = msgs[len(msgs)-1].CommandIndex

			rf.sendApplyMessages(msgs)
			return // Early return to ensure the lock is not re-acquired here
		}
	}
}

func (rf *Raft) sendApplyMessages(msgs []ApplyMsg) {
	for _, msg := range msgs {
		rf.applyCH <- msg
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// Initialize reply defaults
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.Abandon = false
	reply.LogIndex = rf.lastLogIndex()
	reply.LogTerm = rf.lastLogTerm()

	// Election timeout reset and term comparison
	rf.ElectionTimeoutReset()
	rf.persist()

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.mu.Lock()
	// Update state if term from leader is newer
	if args.Term > rf.currentTerm {

		rf.becomeFollower(args.Term)
		rf.persist()

	}
	rf.mu.Unlock()
	// Check for log consistency
	if !rf.isLogConsistent(args, reply) {
		return
	}

	// Append new entries if necessary
	rf.appendEntries(args, reply)

	// Update commitIndex if necessary
	rf.updateCommitIndexOnSuccess(args)

	// Persist state changes
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	//rf.mu.Unlock()
}

// Helper function to become follower and reset state
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

// Helper function to check log consistency
func (rf *Raft) isLogConsistent(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	lastLogIndex := rf.lastLogIndex()

	if args.PrevLogIndex > lastLogIndex || args.PrevLogIndex < rf.indexhelp {
		reply.Success = false
		rf.mu.Unlock()
		return false
	}

	if rf.log[args.PrevLogIndex-rf.indexhelp].Term != args.PrevLogTerm {
		reply.Success = false
		// Find the first index in that term to optimize log backtracking
		idx := args.PrevLogIndex
		for idx > rf.indexhelp && rf.log[idx-rf.indexhelp].Term == rf.log[args.PrevLogIndex-rf.indexhelp].Term {
			idx--
		}
		reply.LogIndex = idx
		reply.LogTerm = rf.log[idx-rf.indexhelp].Term
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	return true
}

// Helper function to append entries
func (rf *Raft) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 {
		// No new entries to append, could be a heartbeat
		return
	}

	// Append any new entries not already in the log
	rf.mu.Lock()
	rf.log = append(rf.log[:args.PrevLogIndex-rf.indexhelp+1], args.Entries...)
	rf.mu.Unlock()
}

// Helper function to update commitIndex on successful AppendEntries
func (rf *Raft) updateCommitIndexOnSuccess(args *AppendEntriesArgs) {
	rf.mu.Lock()
	if rf.commitIndex < args.LeaderCommit {
		// Update commitIndex to the minimum of leaderCommit and index of last new entry
		lastNewIndex := len(rf.log) + rf.indexhelp - 1
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		// Notify that new entries may be committed
		rf.APPLYCHANNEL <- struct{}{}
	}
	rf.mu.Unlock()
}

func (rf *Raft) isLogCompatible(args *AppendEntriesArgs, lastLogIndex, lastLogTerm int) bool {
	if len(args.Entries) == 0 {
		return !(lastLogTerm > args.PrevLogTerm || (lastLogTerm == args.PrevLogTerm && lastLogIndex > args.PrevLogIndex))
	}
	return !(lastLogTerm > args.Entries[len(args.Entries)-1].Term || (lastLogTerm == args.Entries[len(args.Entries)-1].Term && lastLogIndex > args.PrevLogIndex+len(args.Entries)))
}
func (rf *Raft) findConflictIndex(reply *AppendEntriesReply, prevLogIndex int) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if prevLogIndex is beyond the current length of the log.
	if prevLogIndex >= len(rf.log)+rf.indexhelp {
		// If prevLogIndex is outside the current log range, set the reply to the last index and term in the log.
		if len(rf.log) > 0 {
			reply.LogIndex = len(rf.log) + rf.indexhelp - 1
			reply.LogTerm = rf.log[len(rf.log)-1].Term
		} else {
			// Log is empty, respond with zero index and term.
			reply.LogIndex = 0
			reply.LogTerm = 0
		}
		return reply
	}

	// Iterate backwards from the given prevLogIndex to find the first index where the terms differ.
	for idx := prevLogIndex - 1; idx >= 0; idx-- {
		if rf.log[idx-rf.indexhelp].Term != rf.log[prevLogIndex-rf.indexhelp].Term {
			reply.LogIndex = idx + rf.indexhelp
			reply.LogTerm = rf.log[idx-rf.indexhelp].Term
			return reply
		}
	}

	// If no conflict is found from the start of the log to prevLogIndex, return the start.
	reply.LogIndex = rf.indexhelp
	reply.LogTerm = rf.log[0].Term
	if len(rf.log) == 0 {
		// If the log is actually empty.
		reply.LogIndex = 0
		reply.LogTerm = 0
	}
	return reply
}

func (rf *Raft) AppendEntryGoroutine(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		// Immediate leadership and state check
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// Initialize nextIndex for the target server
		nextIndex := rf.nextIndex[server]
		if nextIndex <= rf.indexhelp {
			nextIndex = rf.indexhelp + 1
		}

		// Determine the previous log index and term
		prevLogIndex := nextIndex - 1
		prevLogIndexAdjusted := max(0, prevLogIndex-rf.indexhelp) // Ensure within bounds
		var prevLogTerm int
		if prevLogIndexAdjusted < len(rf.log) {
			prevLogTerm = rf.log[prevLogIndexAdjusted].Term
		}

		// Setup AppendEntries arguments
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[prevLogIndexAdjusted+1:], // slice may be empty
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		// Send AppendEntries RPC
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(server, &args, &reply)

		// Process the response
		rf.mu.Lock()
		if reply.Abandon {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower2(reply.Term) // Assuming this method correctly handles term update and state transition
			rf.ElectionTimeoutReset()
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// Additional check for leadership and term consistency
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if !reply.Success {
			// Adjust nextIndex according to the reply
			if reply.LogIndex >= 0 {
				nextIndex = min(reply.LogIndex+1, rf.indexhelp+1+len(rf.log))
			}
			rf.nextIndex[server] = nextIndex
			rf.persist()
			rf.mu.Unlock()
			return
		} else {
			// On successful reply, update nextIndex and matchIndex
			rf.nextIndex[server] = nextIndex + len(args.Entries)
			rf.matchIndex[server] = nextIndex + len(args.Entries) - 1
			rf.persist()
			if len(args.Entries) > 0 {
				rf.Adjustcommitindex() // Adjust commit index if there were new entries
			}
			rf.mu.Unlock()
			return // Successful replication, exit the loop
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeFollower2(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower

}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) Adjustcommitindex() {
	hasCommit := false
	for i := rf.commitIndex + 1; i <= len(rf.log)+rf.indexhelp-1; i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if hasCommit {
		rf.APPLYCHANNEL <- struct{}{}
	}
}

func (rf *Raft) RequestVoteRPCGoroutine(args RequestVoteArgs, peer int, channel chan bool) bool {
	reply := RequestVoteReply{} //Empty Reply initialization
	rf.sendRequestVote(peer, &args, &reply)
	channel <- reply.VoteGranted
	if args.Term < reply.Term { //if the leader's term is less than the follower's term
		rf.mu.Lock()
		if rf.currentTerm < reply.Term { //if the node's current term is less than the reply.
			rf.currentTerm = reply.Term //update values and revert to follweer
			rf.votedFor = -1
			rf.state = Follower
			rf.ElectionTimeoutReset() //reset the election timeout
			rf.persist()
		}
		rf.persist()
		rf.mu.Unlock()
		return true
	}
	return true
}

func (rf *Raft) RequestVoteRPCtoAll(args RequestVoteArgs, channel chan bool) { //helper function to send all the Request vote RPCS
	for peer := range rf.peers { //for peers in rf's peers
		if rf.me == peer { //if itself, skip
			continue
		}
		go rf.RequestVoteRPCGoroutine(args, peer, channel) //goroutine
	}
}

func (rf *Raft) hbticker() { //ticker that will send out heartbeats
	//Upon election: send initial empty AppendEntries RPCs
	//(heartbeat) to each server; repeat during idle periods to
	//prevent election timeouts
	for {
		select {
		case <-rf.timer_for_heartbeat.C: //every timer for heartbeat time

			rf.timer_for_heartbeat.Stop()                        //stop the timer
			rf.timer_for_heartbeat.Reset(time.Millisecond * 150) //reset the heartbeat
			//some concurrent operation?
			rf.SendFilledAppendEntriesToAll() //send the heartbeat
		case <-rf.killedchannel:
			return
		}
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Initialize a new Raft struct.

	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		// Set initial state and persistent state variables.
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // Start with an empty log; index 0 is a placeholder.
		state:       Follower,
		// Channels and timers.
		applyCH:       applyCh,
		killedchannel: make(chan struct{}),
		APPLYCHANNEL:  make(chan struct{}, 10000), // Buffer to prevent blocking on sends.
		// Initialization of timers with random election timeout to avoid split votes.
		election_timeout:    time.NewTimer(randomElectionTimeout()),
		timer_for_heartbeat: time.NewTimer(time.Millisecond * 100),
	}

	// Read the persisted state from storage.
	rf.readPersist(persister.ReadRaftState())

	rf.lastApplied = rf.indexhelp //initialize to indexhelp
	rf.channelHelp = make(chan ApplyMsg, 10)
	// Start background goroutines.
	go rf.ticker()           // Manages election timeout and triggers elections.
	go rf.hbticker()         // Sends heartbeats to other servers when this server is the leader.
	go rf.tellapplychannel() // Handles applying log entries to the state machine.

	// Return the initialized Raft instance.
	return rf
}

// Helper function to generate a random election timeout to prevent split votes.
func randomElectionTimeout() time.Duration {
	// Election timeouts should typically be between 150ms and 300ms.
	return time.Millisecond*150 + time.Duration(rand.Int63n(150))*time.Millisecond
}
