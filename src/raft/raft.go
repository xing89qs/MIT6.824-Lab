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

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	hasReceivedFromLeader    bool
	hasReceivedFromCandidate bool
	currentTerm              int
	votedFor                 string
	state                    string
	id                       string
	voteCount                int
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int    // candidate's term
	CandidateId string // candidate requesting vote
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
	DPrintf("handleRequestVote in %v from %v", rf.me, args.CandidateId)
	rf.hasReceivedFromCandidate = true
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.BecomeFollower()
	}
	rf.hasReceivedFromCandidate = true
	rf.currentTerm = args.Term
	DPrintf("current %v: state = %v, vote = %v", rf.me, rf.state, rf.votedFor)
	if rf.votedFor == "" || rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("vote granted by %v to %v", rf.me, args.CandidateId)
	}
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
	DPrintf("sendRequestVote from %v to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term        int // leader's term
	LeaderIndex int // leader's id
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("handleAppendEntries in %v from %v", rf.me, args.LeaderIndex)
	rf.hasReceivedFromLeader = true
	rf.votedFor = ""
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.BecomeFollower()
	}
	rf.currentTerm = args.Term
	rf.hasReceivedFromLeader = true
	rf.mu.Unlock()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("sendAppendEntries from %v to %v", rf.me, server)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

const ELECTION_TIMEOUT = 300

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT)+ELECTION_TIMEOUT) * time.Millisecond
}

func (rf *Raft) BecomeFollower() {
	DPrintf("%v become follower.", rf.me)
	rf.hasReceivedFromLeader = false
	rf.hasReceivedFromCandidate = false
	rf.state = FOLLOWER
	rf.votedFor = ""
}

func (rf *Raft) BecomeCandidate() {
	DPrintf("%v become candidate.", rf.me)
	rf.hasReceivedFromLeader = false
	rf.hasReceivedFromCandidate = false
	rf.state = CANDIDATE
	rf.voteCount = 1
	rf.votedFor = rf.id
	rf.currentTerm += 1
	copiedCurrentTerm = rf.currentTerm

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.RLock()
			args := RequestVoteArgs{copiedCurrentTerm, rf.id}
			reply := RequestVoteReply{}
			rf.mu.RUnlock()
			ok := rf.sendRequestVote(id, &args, &reply)
			rf.mu.Lock()
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.BecomeFollower()
				} else {
					if reply.VoteGranted {
						if reply.Term == rf.currentTerm {
							rf.voteCount += 1
						}
					}
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) BecomeLeader() {
	DPrintf("%v become leader.", rf.me)
	rf.hasReceivedFromLeader = false
	rf.hasReceivedFromCandidate = false
	rf.state = LEADER
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.RLock()
			args := AppendEntriesArgs{rf.currentTerm, rf.me}
			reply := AppendEntriesReply{}
			rf.mu.RUnlock()
			ok := rf.sendAppendEntries(id, &args, &reply)
			rf.mu.Lock()
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.BecomeFollower()
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

// go routine that will periorically do leeader election
func (rf *Raft) LeaderElection() {
	for {
		rf.mu.RLock()
		DPrintf("current %v: term = %v, state = %s", rf.me, rf.currentTerm, rf.state)
		state := rf.state
		rf.mu.RUnlock()
		switch state {
		case LEADER:
			rf.mu.Lock()
			rf.BecomeLeader()
			rf.mu.Unlock()
			time.Sleep(time.Duration(200) * time.Millisecond)
		case FOLLOWER:
			rf.mu.Lock()
			if !rf.hasReceivedFromLeader && !rf.hasReceivedFromCandidate {
				rf.BecomeCandidate()
			} else {
				rf.BecomeFollower()
			}
			rf.mu.Unlock()
			time.Sleep(getElectionTimeout())
		case CANDIDATE:
			rf.mu.Lock()
			DPrintf("I'm candidate %v", rf.me)
			if rf.hasReceivedFromLeader {
				rf.BecomeFollower()
			} else {
				DPrintf("received vote: %v", rf.voteCount)
				if rf.voteCount > len(rf.peers)/2 {
					rf.BecomeLeader()
				} else {
					rf.BecomeCandidate()
				}
			}
			rf.mu.Unlock()
			time.Sleep(getElectionTimeout())
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = ""
	rf.id = "server" + strconv.Itoa(rf.me)
	rf.hasReceivedFromLeader = false
	rf.hasReceivedFromCandidate = false
	rf.mu.Unlock()

	go rf.LeaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
