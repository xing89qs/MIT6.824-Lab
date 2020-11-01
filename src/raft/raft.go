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
	CommandData  []byte
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

	lastReceivedFromLeader int64
	lastStartCandidate     int64
	lastHeartbeat          int64
	timeout                int64
	currentTerm            int
	votedFor               string
	state                  string
	id                     string
	voteCount              int
	isDead                 bool

	log           []LogEntry // log entries
	commitIndex   int        // index of highest log entry known to be commited
	lastApplied   int        // index of highest log entry applied to state machine
	nextIndex     []int      // for each server, index of next log entry to send to that server
	matchIndex    []int      // for each server, index of highest log entry known to be replicated on server
	snapshotIndex int        // index offset of the snapshot
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
// if snapshot is null then only safe raft state
func (rf *Raft) PersistSnapShotAndState(snapshot []byte, lastExecuteRaftIndex int) {
	rf.lock("PersistSnapShotAndState")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	if lastExecuteRaftIndex != -1 {
		if lastExecuteRaftIndex >= rf.snapshotIndex {
			// fmt.Printf("[rf %v]: lastExecuteRaftIndex = %v, snapshotIndex = %v, len = %v\n", rf.me, lastExecuteRaftIndex, rf.snapshotIndex, len(rf.log))
			rf.log = rf.log[lastExecuteRaftIndex-rf.snapshotIndex:]
			rf.snapshotIndex = lastExecuteRaftIndex
		}
	}
	l := len(rf.log)
	e.Encode(l)
	e.Encode(rf.snapshotIndex)
	for _, entry := range rf.log {
		e.Encode(entry)
	}
	data := w.Bytes()
	rf.persist(data, snapshot)
	if lastExecuteRaftIndex != -1 {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int, snapshotIndex int, data []byte) {
				for {
					rf.mu.RLock()
					if snapshotIndex != rf.snapshotIndex || rf.state != LEADER {
						rf.mu.RUnlock()
						break
					}
					args := InstallSnapshotArgs{
						Term:                  rf.currentTerm,
						LeaderIndex:           rf.me,
						Snapshot:              data,
						SnapshotIndex:         snapshotIndex,
						FirstSnapShotLogEntry: rf.log[0],
					}
					rf.mu.RUnlock()
					reply := InstallSnapshotReply{}
					// fmt.Printf("%v tries to sendInstallSnapshot to %v, reply = %v\n", rf.me, i, reply)
					if ok := rf.sendInstallSnapshot(i, args, &reply); ok {
						break
					}
					time.Sleep(time.Duration(50) * time.Millisecond)
				}
			}(i, rf.snapshotIndex, snapshot)
		}
	}
	rf.unlock("PersistSnapShotAndState")
}

func (rf *Raft) persist(state []byte, snapshot []byte) {
	if snapshot == nil {
		rf.persister.SaveRaftState(state)
	} else {
		rf.persister.SaveStateAndSnapshot(state, snapshot)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor string
	var l int
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&l) != nil ||
		d.Decode(&snapshotIndex) != nil {
		DPrintf3("Error: Failed to read persisted data.")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.snapshotIndex = snapshotIndex
		logs := make([]LogEntry, l)
		for i := 0; i < l; i++ {
			if d.Decode(&logs[i]) != nil {
				DPrintf3("Error: Failed to read logs.")
			}
		}
		// fmt.Printf("[rf %v] rf.log changed from %v to %v\n", rf.me, len(rf.log), len(logs))
		copy(rf.log, logs)
		// fmt.Printf("[rf %v]prel = %v, afterl = %v, lalala\n", rf.me, len(rf.log), len(logs))
		rf.ApplySnapshot(rf.persister.ReadSnapshot(), rf.snapshotIndex, logs)
		// rf.Log("readPersist: LastLogTerm = %v\n", rf.log[len(rf.log)-1].Term)
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

func (rf *Raft) Log(format string, a ...interface{}) {
	fmt.Printf("[rf %v]: "+format, append([]interface{}{rf.me}, a...)...)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock("RequestVote")
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// rf.Log("Vote from rf %v has larger term %v, currentTerm = %v.\n", args.CandidateId, args.Term, rf.currentTerm)
		rf.unlock("RequestVote")
		return
	} else if args.Term > rf.currentTerm {
		rf.votedFor = ""
		rf.BecomeFollower()
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if rf.votedFor == "" || rf.votedFor == args.CandidateId {
		// rf.Log("Vote from rf %v, args.LastLogTerm = %v, args.LastLogIndex = %v, myLogTerm = %v, myLogIndex = %v\n", args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)
		upToDate := true
		l := len(rf.log)
		if rf.log[l-1].Term > args.LastLogTerm {
			upToDate = false
		} else if rf.log[l-1].Term == args.LastLogTerm &&
			l-1+rf.snapshotIndex > args.LastLogIndex {
			upToDate = false
		}
		if upToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastStartCandidate = NowMilli()
		} else {
			// rf.Log("Not granting vote as log is not up to date on candidate %v, currentLastLogTerm = %v, args.LastLogTerm = %v, currentLastIndex = %v, args.LastLogIndex = %v.\n", args.CandidateId, rf.log[len(rf.log)-1].Term, args.LastLogTerm, len(rf.log)-1+rf.snapshotIndex, args.LastLogIndex)
		}
	}
	// rf.Log("commitIndex = %v\n", rf.commitIndex)
	rf.unlock("RequestVote")
	rf.PersistSnapShotAndState(nil, -1)
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
	Term        int // currentTerm, for leader to update itself
	Success     bool
	LastApplied int
}

type InstallSnapshotArgs struct {
	Term        int //leader's term
	LeaderIndex int // leader's id

	Snapshot              []byte
	SnapshotIndex         int
	FirstSnapShotLogEntry LogEntry
}

type InstallSnapshotReply struct {
	Term    int //currentTerm
	Success bool
}

func (rf *Raft) apply() {
	rf.lock("apply")
	defer rf.unlock("apply")
	low := rf.lastApplied + 1
	if low <= rf.snapshotIndex {
		low = rf.snapshotIndex + 1
	}
	commitIndex := rf.commitIndex
	if commitIndex <= rf.snapshotIndex {
		commitIndex = rf.snapshotIndex
	}
	for i := low; i <= commitIndex; i++ {
		if i > rf.commitIndex || i <= rf.snapshotIndex {
			break
		}
		msg := ApplyMsg{!rf.log[i-rf.snapshotIndex].NoOp, rf.log[i-rf.snapshotIndex].Command, i, nil}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
}

func compareLog(log1 LogEntry, log2 LogEntry) bool {
	return log1.Term == log2.Term && log1.Command == log2.Command
}

func (rf *Raft) ApplySnapshot(data []byte, snapshotIndex int, logs []LogEntry) {
	installSnapshotCommand := ApplyMsg{
		CommandIndex: snapshotIndex,
		Command:      "InstallSnapshot",
		CommandValid: false,
		CommandData:  data,
	}
	rf.applyCh <- installSnapshotCommand
	rf.snapshotIndex = snapshotIndex
	// fmt.Printf("Server %v read snapshot at index %v\n", rf.me, rf.snapshotIndex)
	// fmt.Printf("[rf %v]rf.log changed from %v to %v, haha, rf.snapshot = %v\n", rf.me, len(rf.log), len(logs), rf.snapshotIndex)
	rf.log = make([]LogEntry, len(logs))
	copy(rf.log, logs)
	// fmt.Printf("[rf %v] new rf.log = %v\n", rf.me, len(rf.log))
	rf.lastApplied = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("installsnapshot")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.unlock("installsnapshot")
		return
	} else if args.Term > rf.currentTerm {
		reply.Success = false
		rf.unlock("installsnapshot")
		return
	} else if args.SnapshotIndex <= rf.snapshotIndex {
		reply.Success = true
		rf.unlock("installsnapshot")
		return
	} else if args.SnapshotIndex-rf.snapshotIndex < len(rf.log) &&
		args.FirstSnapShotLogEntry.Term != rf.log[args.SnapshotIndex-rf.snapshotIndex].Term {
		reply.Success = false
		rf.unlock("installsnapshot")
		return
	} else if args.SnapshotIndex-rf.snapshotIndex < len(rf.log) {
		newLog := make([]LogEntry, len(rf.log)-args.SnapshotIndex+rf.snapshotIndex)
		copy(newLog, rf.log[args.SnapshotIndex-rf.snapshotIndex:])
		// fmt.Printf("[rf %v]prel = %v, afterl = %v\n", rf.me, len(rf.log), len(newLog))
		rf.ApplySnapshot(args.Snapshot, args.SnapshotIndex, newLog)
		rf.unlock("installsnapshot")
		reply.Success = true
	} else {
		// fmt.Printf("rf %v InstallSnapshot\n", rf.me)
		// fmt.Printf("[rf %v]prel = %v, afterl = %v\n", rf.me, len(rf.log), 111111)
		rf.ApplySnapshot(args.Snapshot, args.SnapshotIndex, []LogEntry{args.FirstSnapShotLogEntry})
		reply.Success = true
		rf.unlock("installsnapshot")
	}
	rf.PersistSnapShotAndState(args.Snapshot, args.SnapshotIndex)
}

func (rf *Raft) lock(pos string) {
	rf.mu.Lock()
	// fmt.Printf("[rf %v]%v acquire lock!\n", rf.me, pos)
}

func (rf *Raft) unlock(pos string) {
	// fmt.Printf("[rf %v]%v release lock! state = %v, is_dead = %v\n", rf.me, pos, rf.state, rf.isDead)
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	// DPrintf3("%v %v %v", rf.me, args, reply)
	reply.Term = rf.currentTerm
	reply.LastApplied = rf.lastApplied
	if args.Term < rf.currentTerm {
		// rf.Log("Entries from rf %v has smaller term %v, currentTerm = %v.\n", args.LeaderIndex, args.Term, rf.currentTerm)
		rf.unlock("AppendEntries")
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		// rf.Log("Entries from rf %v has larger term %v, currentTerm = %v.\n", args.LeaderIndex, args.Term, rf.currentTerm)
		rf.lastReceivedFromLeader = NowMilli()
		rf.currentTerm = args.Term
		// rf.Log("BecomeFollower\n")
		rf.BecomeFollower()
		rf.unlock("AppendEntries")
		rf.PersistSnapShotAndState(nil, -1)
		reply.Success = false
		return
	} else {
		rf.lastReceivedFromLeader = NowMilli()
		if args.PrevLogIndex >= len(rf.log)+rf.snapshotIndex ||
			(args.PrevLogIndex >= rf.snapshotIndex &&
				args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.snapshotIndex].Term) {
			// if args.PrevLogIndex < len(rf.log)+rf.snapshotIndex {
			// rf.Log("Entries from rf %v has inconsistent term %v from index %v, currentTerm = %v.\n", args.LeaderIndex, args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex-rf.snapshotIndex].Term)
			// }
			rf.unlock("AppendEntries")
			reply.Success = false
			return
		}
		rf.lastReceivedFromLeader = NowMilli()
		rf.BecomeFollower()
		var conflict bool
		conflict = false
		logLen := len(rf.log)
		l := len(args.Entries)
		entries := make([]LogEntry, l)
		copy(entries, args.Entries)
		mxTerm := args.PrevLogTerm
		for i := 0; i < l; i++ {
			mxTerm = MaxInt(mxTerm, entries[i].Term)
			if args.PrevLogIndex+1+i < logLen+rf.snapshotIndex {
				if args.PrevLogIndex+1+i >= rf.snapshotIndex &&
					!compareLog(rf.log[args.PrevLogIndex+1+i-rf.snapshotIndex], entries[i]) {
					rf.log[args.PrevLogIndex+1+i-rf.snapshotIndex] = entries[i]
					conflict = true
				}
			} else {
				rf.log = append(rf.log, entries[i])
			}
		}
		// if conflict || args.PrevLogIndex+l <= logLen+rf.snapshotIndex {
		if conflict {
			rf.log = rf.log[0 : l+args.PrevLogIndex+1-rf.snapshotIndex]
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = MaxInt(rf.commitIndex, MinInt(args.LeaderCommit, l+args.PrevLogIndex))
		}
		// rf.Log("from %v, argsTerm = %v, LastLogTerm = %v, mxTerm = %v, append = %v, len = %v\n", args.LeaderIndex, args.Term, rf.log[len(rf.log)-1].Term, mxTerm, args.PrevLogIndex+l, logLen+rf.snapshotIndex)
	}
	rf.unlock("AppendEntries")
	rf.PersistSnapShotAndState(nil, -1)
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	rf.lock("Start")
	defer func() {
		if rf.state == LEADER {
			rf.lastHeartbeat = 0 // Trigger Appendentries
		}
		rf.unlock("Start")
	}()
	return rf.startCommand(command, false)
}

func (rf *Raft) startCommand(command interface{}, noop bool) (int, int, bool) {
	isLeader := (rf.state == LEADER)
	index := len(rf.log) + rf.snapshotIndex
	term := rf.currentTerm
	entry := LogEntry{index, command, noop, term}
	if isLeader {
		rf.log = append(rf.log, entry)
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
	rf.lock("Kill")
	defer rf.unlock("Kill")
	rf.state = DEAD
	rf.isDead = true
}

func NowMilli() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

const ELECTION_TIMEOUT = 300
const SMALL_SLEEP_GAP = time.Duration(10) * time.Millisecond
const HEARTBEAT_TIMEOUT = 100

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT) + ELECTION_TIMEOUT)
}

func (rf *Raft) BecomeFollower() {
	DPrintf("%v become follower.", rf.me)
	rf.state = FOLLOWER
	rf.voteCount = 0
}

func (rf *Raft) BecomeCandidate() {
	rf.lock("BecomeCandidate")
	if rf.timeout >= NowMilli()-rf.lastStartCandidate {
		rf.unlock("BecomeCandidate")
		return
	}
	rf.state = CANDIDATE
	rf.voteCount = 1
	rf.votedFor = rf.id
	rf.currentTerm += 1
	rf.timeout = int64(getElectionTimeout())
	// rf.Log("Become candidate, currentTerm = %v, time: %v\n", rf.currentTerm, time.Now())
	copiedCurrentTerm := rf.currentTerm
	rf.lastStartCandidate = NowMilli()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.RLock()
			if rf.state != CANDIDATE || rf.currentTerm != copiedCurrentTerm {
				rf.mu.RUnlock()
				return
			}
			l := len(rf.log)
			args := RequestVoteArgs{copiedCurrentTerm,
				rf.id, l - 1 + rf.snapshotIndex, rf.log[l-1].Term}
			reply := RequestVoteReply{}
			rf.mu.RUnlock()
			ok := rf.sendRequestVote(id, &args, &reply)
			if ok {
				// rf.Log("try to send vote rpc from %v to %v, ret: %v, sucess: %v, args.Term = %v\n", rf.me, id, ok, reply.VoteGranted, args.Term)
			}
			rf.lock("BecomeCandidate")
			if rf.currentTerm != args.Term {
				rf.unlock("BecomeCandidate")
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
						}
						if rf.voteCount > len(rf.peers)/2 {
							rf.matchIndex = make([]int, len(rf.peers))
							rf.nextIndex = make([]int, len(rf.peers))
							l := len(rf.log)
							for i, _ := range rf.peers {
								rf.nextIndex[i] = l
							}
							rf.BecomeLeader()
						}
					}
				}
			}
			rf.unlock("BecomeCandidate")
			rf.PersistSnapShotAndState(nil, -1)
		}(i)
	}
	rf.unlock("BecomeCandidate")

}

func (rf *Raft) BecomeLeader() {
	rf.state = LEADER
	rf.lastHeartbeat = NowMilli()
	for i := len(rf.log) - 1; i+rf.snapshotIndex > rf.commitIndex && i >= 0; i-- {
		cnt := 0
		for j, _ := range rf.peers {
			if rf.me == j {
				continue
			}
			if i+rf.snapshotIndex <= rf.matchIndex[j] && rf.log[i].Term == rf.currentTerm {
				cnt += 1
			}
		}
		// rf.Log("Leader. index: %v cnt: %v, term: %v, rf.currentTerm: %v\n", i, cnt, rf.log[i].Term, rf.currentTerm)
		if cnt >= len(rf.peers)/2 {
			rf.commitIndex = i + rf.snapshotIndex
			break
		}
	}
	// DPrintf3("rf_id: %v, rf.commitIndex = %v ", rf.me, rf.commitIndex)
	//	for i, _ := range rf.peers {
	//		fmt.Printf("%d ", rf.matchIndex[i])
	//	}
	// 	fmt.Printf(" at master [%v], commitIndex = %v, currentTerm = %v, len(log) = %v, lastTerm = %v\n", rf.me, rf.commitIndex, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.lock("BecomeLeader")
			if rf.state != LEADER {
				rf.unlock("BecomeLeader")
				return
			}
			var entry []LogEntry
			var args AppendEntriesArgs
			// DPrintf3("Try entry: len = %v, id = %v, nextIndex =  %v, matchIndex = %v", len(rf.log), id, rf.nextIndex[id], rf.matchIndex[id])
			rf.nextIndex[id] = MaxInt(MinInt(rf.nextIndex[id], len(rf.log)+rf.snapshotIndex), rf.snapshotIndex+1)
			entry = make([]LogEntry, len(rf.log)+rf.snapshotIndex-rf.nextIndex[id])
			for j, e := range rf.log[rf.nextIndex[id]-rf.snapshotIndex:] {
				entry[j].Index = e.Index
				entry[j].Command = e.Command
				entry[j].Term = e.Term
				entry[j].NoOp = e.NoOp
			}
			args = AppendEntriesArgs{rf.currentTerm, rf.me,
				rf.nextIndex[id] - 1, rf.log[rf.nextIndex[id]-1-rf.snapshotIndex].Term,
				entry, rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.unlock("BecomeLeader1")
			ok := rf.sendAppendEntries(id, args, &reply)
			rf.lock("BecomeLeader2")
			if rf.currentTerm != args.Term {
				rf.unlock("BecomeLeader2")
				return
			}
			if ok {
				// rf.Log("try to send rpc from %v to %v with %v logs, ret: %v, sucess: %v, leaderCommit: %d, LastLogIndex = %v, lastIndexTerm = %v\n", rf.me, id, len(args.Entries), ok, reply.Success, rf.commitIndex, args.PrevLogIndex, args.PrevLogTerm)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.BecomeFollower()
				} else if reply.Success {
					if rf.state != LEADER {
						// fmt.Printf("%v is not leader now, is %s\n", rf.me, rf.state)
						rf.unlock("BecomeLeader3")
						return
					}
					l := len(args.Entries)
					if l > 0 {
						// fmt.Printf("%v %v args.Entries[l-1].Index = %v\n", rf.me, id, args.Entries[l-1].Index)
						rf.matchIndex[id] = MaxInt(rf.matchIndex[id], args.Entries[l-1].Index)
						rf.nextIndex[id] = rf.matchIndex[id] + 1
					} else {
						rf.nextIndex[id] = MaxInt(rf.matchIndex[id]+1, len(rf.log)-1+rf.snapshotIndex)
					}
				} else {
					// rf.nextIndex[id] = MaxInt((rf.nextIndex[id]-rf.snapshotIndex)/2, 1) + rf.snapshotIndex
					rf.nextIndex[id] = reply.LastApplied
				}
			}
			rf.unlock("BecomeLeader4")
			rf.PersistSnapShotAndState(nil, -1)
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
		if rf.isDead {
			rf.mu.RUnlock()
			break
		}
		rf.mu.RUnlock()
		rf.apply()
		rf.PersistSnapShotAndState(nil, -1)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// go routine that will periorically do leeader election
func (rf *Raft) LeaderElection() {
LEADER_ELECTION_LOOP:
	for {
		rf.lock("LeaderElection")
		// fmt.Printf("%v is %v, term = %v\n", rf.me, rf.state, rf.currentTerm)
		if rf.isDead {
			fmt.Printf("[rf %v] Killed.\n", rf.me)
			break LEADER_ELECTION_LOOP
		}
		//if rf.state == LEADER {
		// DPrintf3("current %v: term = %v, state = %s, commit = %v, log = %v", rf.me, rf.currentTerm, rf.state, rf.commitIndex, len(rf.log))
		// }
		// DPrintf3("current snapshotIndex[%v]: %d", rf.me, rf.snapshotIndex)
		state := rf.state
		switch state {
		case LEADER:
			if HEARTBEAT_TIMEOUT < NowMilli()-rf.lastHeartbeat {
				rf.BecomeLeader()
			}
			rf.unlock("LeaderElection_leader")
			rf.PersistSnapShotAndState(nil, -1)
			time.Sleep(ELECTION_TIMEOUT)
		case FOLLOWER:
			if rf.timeout < NowMilli()-rf.lastReceivedFromLeader && rf.timeout < NowMilli()-rf.lastStartCandidate {
				rf.unlock("LeaderElection_follower")
				rf.BecomeCandidate()
			} else {
				rf.BecomeFollower()
				rf.unlock("LeaderElection_follower")
			}
			time.Sleep(SMALL_SLEEP_GAP)
		case CANDIDATE:
			rf.unlock("LeaderElection_candidate")
			rf.BecomeCandidate()
			time.Sleep(SMALL_SLEEP_GAP)
		case DEAD:
			fmt.Printf("[rf %v] Killed.\n", rf.me)
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
	rf.lastStartCandidate = 0
	rf.timeout = 0
	rf.lastHeartbeat = 0
	rf.isDead = false

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

	// initialize from state persisted before a crash
	rf.readPersist()

	go rf.LeaderElection()
	go rf.ApplyLogEntry()

	return rf
}
