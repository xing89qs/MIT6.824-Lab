package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key          string
	Value        string
	Command      string
	SeriesNumber int
	ClientId     int64
}

type OpKey struct {
	ClientId     int64
	SeriesNumber int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	quit    chan int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data                    map[string]string
	lastExecuteSerialNumber map[int64]int
	raftCurrentCommitTerm   int
	commitChannel           map[OpKey]chan int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = "This is not the leader!"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	if kv.raftCurrentCommitTerm < term {
		reply.Err = "Leader might contain stale data."
		return
	}
	key := args.Key
	value := kv.data[key]
	reply.Value = value
	reply.Err = ""
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	command := args.Op
	seriesNumber := args.SeriesNumber
	op := Op{key, value, command, seriesNumber, args.ClientId}
	opkey := OpKey{args.ClientId, args.SeriesNumber}
	DPrintf("%s: %s %s", op.Command, op.Key, op.Value)

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	if !isLeader {
		reply.Err = "This is not the leader!"
		return
	}
	if args.SeriesNumber <= kv.lastExecuteSerialNumber[args.ClientId] {
		return
	} else {
		if _, ok := kv.commitChannel[opkey]; !ok {
			kv.commitChannel[opkey] = make(chan int, 1)
		}
		_, _, isLeader = kv.rf.Start(op)
	}
	select {
	case <-kv.commitChannel[opkey]:
		reply.Err = ""
	case <-time.After(500 * time.Millisecond):
		DPrintf("timeout!")
		reply.Err = "Timeout."
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.quit <- 0
}

func (kv *KVServer) execute(op *Op) {
	DPrintf("Execute")
	value, ok := kv.data[op.Key]
	if !ok {
		value = ""
	}
	if op.Command == APPEND {
		value = value + op.Value
	} else if op.Command == PUT {
		value = op.Value
	} else {
		log.Printf("Invalid command %s!", op.Command)
	}
	kv.data[op.Key] = value
	opkey := OpKey{op.ClientId, op.SeriesNumber}
	kv.commitChannel[opkey] <- 1
	kv.lastExecuteSerialNumber[op.ClientId] = op.SeriesNumber
}

func (kv *KVServer) monitorApplyCh(applyCh chan raft.ApplyMsg, quit chan int) {
loop:
	for {
		select {
		case msg := <-applyCh:
			switch msg.Command.(type) {
			case Op:
				op := msg.Command.(Op)
				kv.execute(&op)
			case raft.CommandNoOp:
				kv.raftCurrentCommitTerm = msg.Command.(raft.CommandNoOp).Term
			default:
				log.Printf("%s doesn't have type Op.", msg.Command)
			}
		case <-quit:
			break loop
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.quit = make(chan int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.commitChannel = make(map[OpKey]chan int)
	kv.lastExecuteSerialNumber = make(map[int64]int)

	// You may need initialization code here.
	go kv.monitorApplyCh(kv.applyCh, kv.quit)

	return kv
}
