package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int         // index of the command
	Command interface{} // command for the state machine
	NoOp    bool
	Term    int // term when entry was received by leader
}

type CommandNoOp struct {
	Term  int
	Value int
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	DEAD      = "dead"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastReceivedFromLeader    int64
	lastReceivedFromCandidate int64
	currentTerm               int
	votedFor                  string
	state                     string
	id                        string
	voteCount                 int

	log          []LogEntry // log entries
	commitIndex  int        // index of highest log entry known to be commited
	lastApplied  int        // index of highest log entry applied to state machine
	nextIndex    []int      // for each server, index of next log entry to send to that server
	matchIndex   []int      // for each server, index of highest log entry known to be replicated on server
	hasStartNoOp bool       // whether has start a no-op at the beginning of each term when it becomes leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	rf.mu.RUnlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	DPrintf2("Persist")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	l := len(rf.log)
	e.Encode(l)
	for _, entry := range rf.log {
		e.Encode(entry)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf2("Save")
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor string
	var l int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&l) != nil {
		DPrintf2("Error: Failed to read persisted data.")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		logs := make([]LogEntry, l)
		for i := 0; i < l; i++ {
			if d.Decode(&logs[i]) != nil {
				DPrintf2("Error: Failed to read logs.")
			}
		}
		rf.log = logs
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int    // candidate's term
	CandidateId  string // candidate requesting vote
	LastLogIndex int    // index of candidate's last log entry
	LastLogTerm  int    // term of candidate's log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// DPrintf3("handleRequestVote in %v from %v", rf.me, args.CandidateId)
	rf.lastReceivedFromCandidate = NowMilli()
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.votedFor = ""
		rf.BecomeFollower()
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	DPrintf2("current %v: state = %v, vote = %v", rf.me, rf.state, rf.votedFor)
	if rf.votedFor == "" || rf.votedFor == args.CandidateId {
		upToDate := true
		if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
			upToDate = false
		} else if rf.log[len(rf.log)-1].Term == args.LastLogTerm &&
			len(rf.log)-1 > args.LastLogIndex {
			upToDate = false
		}
		if upToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			DPrintf2("vote granted by %v to %v", rf.me, args.CandidateId)
		} else {
			DPrintf("%v: Not granting vote as log is not up to date on candidate %v.", rf.me, args.CandidateId)
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf3("sendRequestVote from %v to %v,  %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term        int // leader's term
	LeaderIndex int // leader's id

	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex
	Entries      []LogEntry // log entries
	LeaderCommit int        // leader's commit index
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

func (rf *Raft) apply() {
	rf.mu.RLock()
	low := rf.lastApplied + 1
	commitIndex := rf.commitIndex
	rf.mu.RUnlock()
	for i := low; i <= commitIndex; i++ {
		rf.mu.RLock()
		// DPrintf3("%v: commit log %v. state = %s", rf.me, rf.log[i], rf.state)
		msg := ApplyMsg{!rf.log[i].NoOp, rf.log[i].Command, i}
		rf.mu.RUnlock()
		rf.applyCh <- msg
	}
	rf.mu.Lock()
	rf.lastApplied = commitIndex
	rf.mu.Unlock()
}

func compareLog(log1 LogEntry, log2 LogEntry) bool {
	DPrintf3("%v %v %v", log1.Command, log2.Command, log1.Command == log2.Command)
	return log1.Term == log2.Term && log1.Command == log2.Command
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// DPrintf3("%v %v %v", rf.me, args, reply)
	DPrintf2("handleAppendEntries in %v from %v", rf.me, args.LeaderIndex)
	rf.lastReceivedFromLeader = NowMilli()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.BecomeFollower()
		rf.persist()
		rf.mu.Unlock()
		reply.Success = false
		return
	} else if args.PrevLogIndex >= len(rf.log) ||
		args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		rf.mu.Unlock()
		reply.Success = false
		return
	} else {
		var conflict bool
		conflict = false
		logLen := len(rf.log)
		l := len(args.Entries)
		entries := make([]LogEntry, l)
		copy(entries, args.Entries)
		for i := 0; i < l; i++ {
			if args.PrevLogIndex+1+i < logLen {
				if !compareLog(rf.log[args.PrevLogIndex+1+i], entries[i]) {
					rf.log[args.PrevLogIndex+1+i] = entries[i]
					conflict = true
				}
			} else {
				rf.log = append(rf.log, entries[i])
			}
		}
		if conflict {
			rf.log = rf.log[0 : l+args.PrevLogIndex+1]
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = MaxInt(rf.commitIndex, MinInt(args.LeaderCommit, l+args.PrevLogIndex))
		}
	}
	rf.persist()
	rf.mu.Unlock()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrintf3("sendAppendEntries from %v to %v, %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.startCommand(command, false)
}

func (rf *Raft) startCommand(command interface{}, noop bool) (int, int, bool) {
	isLeader := (rf.state == LEADER)
	index := len(rf.log)
	term := rf.currentTerm
	entry := LogEntry{index, command, noop, term}
	if isLeader {
		rf.log = append(rf.log, entry)
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.state = DEAD
	rf.mu.Unlock()
}

func NowMilli() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

const ELECTION_TIMEOUT = 300
const SMALL_SLEEP_GAP = time.Duration(10) * time.Millisecond

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT) + ELECTION_TIMEOUT)
}

func (rf *Raft) BecomeFollower() {
	DPrintf("%v become follower.", rf.me)
	rf.state = FOLLOWER
	rf.voteCount = 0
}

func (rf *Raft) BecomeCandidate() {
	////  DPrintf3("%v become candidate.", rf.me)
	rf.state = CANDIDATE
	rf.voteCount = 1
	rf.votedFor = rf.id
	rf.currentTerm += 1
	copiedCurrentTerm := rf.currentTerm
	rf.lastReceivedFromCandidate = NowMilli()
	rf.persist()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.RLock()
			l := len(rf.log)
			args := RequestVoteArgs{copiedCurrentTerm,
				rf.id, l - 1, rf.log[l-1].Term}
			reply := RequestVoteReply{}
			rf.mu.RUnlock()
			ok := rf.sendRequestVote(id, &args, &reply)
			rf.mu.Lock()
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.BecomeFollower()
				} else if rf.state == CANDIDATE {
					if reply.VoteGranted {
						if reply.Term == rf.currentTerm {
							rf.voteCount += 1
							// DPrintf3("%v received vote from %v", rf.me, id)
						}
					}
				}
				rf.persist()
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) BecomeLeader() {
	// DPrintf3("%v become leader, commitIndex = %v", rf.me, rf.commitIndex)
	//for i, _ := range rf.nextIndex {
	//		DPrintf2("%v %v", rf.nextIndex[i], rf.matchIndex[i])
	//}
	rf.state = LEADER
	// rf.StartNoOp()
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		cnt := 0
		for j, _ := range rf.peers {
			if rf.me == j {
				continue
			}
			if i <= rf.matchIndex[j] && rf.log[i].Term == rf.currentTerm {
				cnt += 1
			}
		}
		if cnt >= len(rf.peers)/2 {
			rf.commitIndex = i
			break
		}
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.RLock()
			var entry []LogEntry
			var args AppendEntriesArgs
			DPrintf2("Try entry: len = %v, id =  %v", len(rf.log), rf.nextIndex[id])
			entry = make([]LogEntry, len(rf.log)-rf.nextIndex[id])
			for j, e := range rf.log[rf.nextIndex[id]:] {
				entry[j].Index = e.Index
				entry[j].Command = e.Command
				entry[j].Term = e.Term
				entry[j].NoOp = e.NoOp
			}
			args = AppendEntriesArgs{rf.currentTerm, rf.me,
				rf.nextIndex[id] - 1, rf.log[rf.nextIndex[id]-1].Term,
				entry, rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.mu.RUnlock()
			ok := rf.sendAppendEntries(id, args, &reply)
			rf.mu.Lock()
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			// DPrintf3("try to send rpc from %v to %v with %v logs, %v", rf.me, id, len(args.Entries), ok)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.BecomeFollower()
				} else if reply.Success {
					if rf.state != LEADER {
						fmt.Printf("%v is not leader now, is %s\n", rf.me, rf.state)
						rf.mu.Unlock()
						return
					}
					l := len(args.Entries)
					if l > 0 {
						rf.matchIndex[id] = MaxInt(rf.matchIndex[id], args.Entries[l-1].Index)
						rf.nextIndex[id] = rf.matchIndex[id] + 1
					}
				} else {
					rf.nextIndex[id] = MaxInt(rf.nextIndex[id]/2, 1)
				}
				rf.persist()
			}
			rf.mu.Unlock()
		}(i)
	}
}

func MinInt(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	} else {
		return y
	}
}

func (rf *Raft) ApplyLogEntry() {
	for {
		rf.mu.RLock()
		if rf.state == DEAD {
			rf.mu.RUnlock()
			break
		}
		rf.mu.RUnlock()
		rf.apply()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) StartNoOp() {
	// start no-op at the beginning of this term
	if !rf.hasStartNoOp {
		rf.hasStartNoOp = true
		DPrintf3("start no-op command")
		rf.startCommand(nil, true)
	}
}

// go routine that will periorically do leeader election
func (rf *Raft) LeaderElection() {
LEADER_ELECTION_LOOP:
	for {
		rf.mu.Lock()
		//DPrintf3("current %v: term = %v, state = %s, commit = %v, log = %v", rf.me, rf.currentTerm, rf.state, rf.commitIndex, len(rf.log))
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case LEADER:
			rf.mu.Lock()
			rf.BecomeLeader()
			rf.mu.Unlock()
			time.Sleep(time.Duration(120) * time.Millisecond)
		case FOLLOWER:
			rf.mu.Lock()
			timeout := int64(getElectionTimeout())
			lastReceivedFromLeader := rf.lastReceivedFromLeader
			lastReceivedFromCandidate := rf.lastReceivedFromCandidate
			rf.mu.Unlock()
			for {
				rf.mu.Lock()
				if lastReceivedFromLeader != rf.lastReceivedFromLeader ||
					lastReceivedFromCandidate != rf.lastReceivedFromCandidate {
					rf.BecomeFollower()
					rf.mu.Unlock()
					break
				}
				if timeout < NowMilli()-Max(lastReceivedFromLeader, lastReceivedFromCandidate) {
					rf.BecomeCandidate()
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				time.Sleep(SMALL_SLEEP_GAP)
			}
		case CANDIDATE:
			rf.mu.Lock()
			lastReceivedFromLeader := rf.lastReceivedFromLeader
			lastReceivedFromCandidate := rf.lastReceivedFromCandidate
			timeout := int64(getElectionTimeout())
			rf.mu.Unlock()
			for {
				rf.mu.Lock()
				if lastReceivedFromLeader != rf.lastReceivedFromLeader {
					rf.BecomeFollower()
					rf.mu.Unlock()
					break
				}
				if rf.state != CANDIDATE {
					rf.mu.Unlock()
					break
				}
				if timeout < NowMilli()-lastReceivedFromCandidate {
					rf.BecomeCandidate()
					rf.mu.Unlock()
					break
				}
				DPrintf("received vote: %v", rf.voteCount)
				if rf.voteCount > len(rf.peers)/2 {
					rf.matchIndex = make([]int, len(rf.peers))
					rf.nextIndex = make([]int, len(rf.peers))
					l := len(rf.log)
					for i, _ := range rf.peers {
						rf.nextIndex[i] = l
					}
					rf.hasStartNoOp = false
					rf.BecomeLeader()
				}
				rf.mu.Unlock()
				time.Sleep(SMALL_SLEEP_GAP)
			}
		case DEAD:
			break LEADER_ELECTION_LOOP
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = ""
	rf.voteCount = 0
	rf.id = "server" + strconv.Itoa(rf.me)
	rf.lastReceivedFromLeader = 0
	rf.lastReceivedFromCandidate = 0
	rf.hasStartNoOp = false

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{0, nil, true, 0}
	l := len(peers)
	rf.nextIndex = make([]int, l, l)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	rf.matchIndex = make([]int, l, l)
	rf.mu.Unlock()

	go rf.LeaderElection()
	go rf.ApplyLogEntry()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
