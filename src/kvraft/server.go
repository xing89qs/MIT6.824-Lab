package kvraft

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

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
	Key     string
	Value   string
	Command string

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
	persister    *raft.Persister

	// Your definitions here.
	data                    map[string]string
	lastExecuteSerialNumber map[int64]int
	commitChannel           map[OpKey]chan int
	lastExecuteRaftIndex    int
}

func (kv *KVServer) lk(s string) {
	// fmt.Printf("[kv %v]lock %s\n", kv.me, s)
}

func (kv *KVServer) ulk(s string) {
	// fmt.Printf("[kv %v] unlock %s\n", kv.me, s)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = "This is not the leader!"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	opkey := OpKey{args.ClientId, args.SeriesNumber}
	kv.lk("get")
	kv.mu.Lock()
	kv.lk("in get")

	if lastExecuteSerialNumber, ok := kv.lastExecuteSerialNumber[args.ClientId]; ok && args.SeriesNumber <= lastExecuteSerialNumber {
		key := args.Key
		value := kv.data[key]
		reply.Value = value
		reply.Err = ""
		kv.mu.Unlock()
		kv.ulk("get")
		return
	} else {
		if _, ok := kv.commitChannel[opkey]; !ok {
			kv.commitChannel[opkey] = make(chan int, 1)
		}
		op := Op{args.Key, "", "Get", args.SeriesNumber, args.ClientId}
		kv.mu.Unlock()
		kv.ulk("get")
		kv.rf.Start(op)
		kv.mu.Lock()
		// fmt.Printf("Start %v on kv %v, index = %v, term=%v, isLeader=%v\n", op, kv.me, index, term, isLeader)
		kv.mu.Unlock()
		kv.checkIfSnapShot()
		kv.mu.Lock()
	}
	commitChannel := kv.commitChannel[opkey]
	kv.mu.Unlock()
	select {
	case <-commitChannel:
		reply.Err = ""
		kv.mu.Lock()
		key := args.Key
		value := kv.data[key]
		reply.Value = value
		reply.Err = ""
		// DPrintf("get key = %s, ClientId=%v, seriesNumber= %v, value=%v", args.Key, args.ClientId, args.SeriesNumber, value)
		kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		DPrintf("Get %v timeout!", args.Key)
		reply.Err = "Operation timeout!"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	command := args.Op
	seriesNumber := args.SeriesNumber
	op := Op{key, value, command, seriesNumber, args.ClientId}
	opkey := OpKey{args.ClientId, args.SeriesNumber}
	kv.lk("append")
	kv.mu.Lock()
	kv.lk("in append")
	kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	reply.WrongLeader = !isLeader
	// fmt.Printf("server reply: %v\n", reply)
	if !isLeader {
		reply.Err = "This is not the leader!"
		kv.mu.Unlock()
		kv.ulk("append")
		return
	}
	if lastExecuteSerialNumber, ok := kv.lastExecuteSerialNumber[args.ClientId]; ok && args.SeriesNumber <= lastExecuteSerialNumber {
		reply.Err = ""
		kv.mu.Unlock()
		kv.ulk("append")
		return
	} else {
		if _, ok := kv.commitChannel[opkey]; !ok {
			kv.commitChannel[opkey] = make(chan int, 1)
		}
		kv.mu.Unlock()
		kv.ulk("append")
		kv.rf.Start(op)
		kv.mu.Lock()
		kv.lk("append1")
		// fmt.Printf("Start %v on kv %v, index = %v, term=%v, isLeader=%v\n", op, kv.me, index, term, isLeader)
		// fmt.Printf("%v\n", kv.lastExecuteSerialNumber[args.ClientId])
		kv.mu.Unlock()
		kv.checkIfSnapShot()
		kv.mu.Lock()
	}
	commitChannel := kv.commitChannel[opkey]
	kv.mu.Unlock()
	kv.ulk("append")
	select {
	case <-commitChannel:
		reply.Err = ""
	case <-time.After(100 * time.Millisecond):
		reply.Err = "Operation timeout!"
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	// fmt.Printf("kill %v\n", kv.me)
	// DPrintf("kill %v", kv.me)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.quit <- 0
}

func (kv *KVServer) execute(op *Op, index int) {
	kv.lk("execute")
	kv.mu.Lock()
	kv.lk("in execute")
	defer func() {
		kv.mu.Unlock()
		kv.ulk("execute")
	}()
	if lastExecuteSerialNumber, ok := kv.lastExecuteSerialNumber[op.ClientId]; ok && op.SeriesNumber <= lastExecuteSerialNumber {
		DPrintf("duplicate command %v on kv %v", op, kv.me)
		return
	}
	if kv.lastExecuteRaftIndex >= index {
		DPrintf("Already executed command %v on kv %v", op, kv.me)
		return
	}
	// fmt.Printf("[kv %v] Execute %v on kv %v at index %v\n", kv.me, op, kv.me, index)
	value, ok := kv.data[op.Key]
	opkey := OpKey{op.ClientId, op.SeriesNumber}
	if !ok {
		value = ""
	}
	if op.Command == APPEND {
		value = value + op.Value
	} else if op.Command == PUT {
		value = op.Value
	} else if op.Command == GET {
		commitChannel, ok := kv.commitChannel[opkey]
		kv.lastExecuteSerialNumber[op.ClientId] = op.SeriesNumber
		DPrintf("Set lastExecuteSerialNumber %v to %v on %v", op.ClientId, op.SeriesNumber, kv.me)
		kv.lastExecuteRaftIndex = index
		if ok {
			select {
			case commitChannel <- 1:
			default:
			}
		}
		return
	} else {
		log.Printf("Invalid command %s!", op.Command)
		return
	}
	kv.data[op.Key] = value
	kv.lastExecuteSerialNumber[op.ClientId] = op.SeriesNumber
	commitChannel, ok := kv.commitChannel[opkey]
	DPrintf("now key=\"%s\", value=\"%s\"", op.Key, value)
	kv.lastExecuteRaftIndex = index
	if ok {
		select {
		case commitChannel <- 1:
		default:
		}
	}
	// fmt.Printf("[kv %v]: Wait to checksnapshot\n", kv.me)
	// kv.checkIfSnapShot()
	// fmt.Printf("[kv %v]: Done checksnapshot\n", kv.me)
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	// fmt.Printf("kv server %v start to read snapshot\n", kv.me)
	if data == nil || len(data) < 1 {
		return
	}
	kv.lk("read snapshot")
	kv.mu.Lock()
	kv.lk("in read snapshot")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// log.Printf("kv %v read data, len = %v\n", kv.me, len(data))
	if d.Decode(&kv.data) != nil || d.Decode(&kv.lastExecuteSerialNumber) != nil || d.Decode(&kv.lastExecuteRaftIndex) != nil {
		log.Fatalf("Error: Failed to read snapshot, kv = %v, len = %v!", kv.me, len(data))
	}
	// fmt.Printf("[kv %v] change to lastExecuteRaftIndex: %v\n", kv.me, kv.lastExecuteRaftIndex)
	// fmt.Printf("kv server %v read map: %v\n", kv.me, kv.data)
	kv.mu.Unlock()
	kv.ulk("read snapshot")
}

func (kv *KVServer) checkIfSnapShot() {
	kv.mu.Lock()
	kv.lk("checkIfSnapShot")
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)
		encoder.Encode(kv.data)
		encoder.Encode(kv.lastExecuteSerialNumber)
		encoder.Encode(kv.lastExecuteRaftIndex)
		data := writer.Bytes()
		lastExecuteRaftIndex := kv.lastExecuteRaftIndex
		kv.rf.PersistSnapShotAndState(data, lastExecuteRaftIndex)
		// fmt.Printf("KV %v save data %v at index %v\n", kv.me, kv.data, kv.lastExecuteRaftIndex)
	}
	kv.ulk("checkIfSnapShot")
	kv.mu.Unlock()
}

func (kv *KVServer) monitorApplyCh(applyCh chan raft.ApplyMsg, persister *raft.Persister) {
loop:
	for {
		select {
		case msg := <-applyCh:
			// fmt.Printf("[kv %v]: msg = %v\n", kv.me, msg)
			switch msg.Command.(type) {
			case Op:
				op := msg.Command.(Op)
				kv.execute(&op, msg.CommandIndex)
			case string:
				command := msg.Command.(string)
				if command != "InstallSnapshot" {
					log.Fatalf("Unknown command %s!\n", command)
				}
				kv.ReadSnapshot(msg.CommandData)
			default:
				log.Printf("%s doesn't have type Op.", msg.Command)
			}
			// fmt.Printf("[kv %v]: msg = %v, done\n", kv.me, msg)
		case <-kv.quit:
			break loop
		}
		kv.checkIfSnapShot()
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

	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.quit = make(chan int, 1)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.commitChannel = make(map[OpKey]chan int)
	kv.lastExecuteSerialNumber = make(map[int64]int)

	// You may need initialization code here.
	go kv.monitorApplyCh(kv.applyCh, persister)

	return kv
}
