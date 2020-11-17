package shardmaster

import (
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"sort"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	quit    chan int

	// Your data here.

	configs                 []Config // indexed by config num
	lastExecuteSerialNumber map[int64]int
	commitChannel           map[OpKey]chan Config
	lastExecuteRaftIndex    int
}

const (
	JOIN     = "Join"
	LEAVE    = "Leave"
	MOVE     = "Move"
	RECONFIG = "Reconfig"
)

type Op struct {
	// Your data here.
	Param interface{}

	SeriesNumber int
	ClientId     int64
}

type OpKey struct {
	SeriesNumber int
	ClientId     int64
}

type JoinParam struct {
	Servers map[int][]string // new GID -> servers mapping
}

type QueryParam struct {
	Num int
}

type MoveParam struct {
	Shard int
	GID   int
}

type LeaveParam struct {
	GID []int
}

func (sm *ShardMaster) lock(s string) {
	sm.mu.Lock()
	// fmt.Printf("[server %d] Lock %s\n", sm.me, s)
}

func (sm *ShardMaster) unlock(s string) {
	// fmt.Printf("[server %d] Unlock %s\n", sm.me, s)
	sm.mu.Unlock()
}

func (sm *ShardMaster) start(operation string, op Op) chan Config {
	opkey := OpKey{op.SeriesNumber, op.ClientId}
	sm.lock(operation)
	if lastExecuteSerialNumber, ok := sm.lastExecuteSerialNumber[op.ClientId]; ok && op.SeriesNumber <= lastExecuteSerialNumber {
		sm.unlock(operation)
		return nil
	} else {
		if _, ok := sm.commitChannel[opkey]; !ok {
			sm.commitChannel[opkey] = make(chan Config, 1)
		}
		sm.unlock(operation)
		fmt.Printf("[server %v] Start op %s: %v\n", sm.me, operation, op)
		sm.rf.Start(op)
		sm.lock(operation + "1")
	}
	var commitChannel chan Config
	commitChannel = sm.commitChannel[opkey]
	sm.unlock(operation + "1")
	return commitChannel
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = "This is not the leader!"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	joinServer := make(map[int][]string)
	for k, v := range args.Servers {
		joinServer[k] = v
	}
	param := JoinParam{joinServer}
	op := Op{param, args.SeriesNumber, args.ClientId}
	commitChannel := sm.start("Join", op)
	select {
	case <-commitChannel:
		reply.Err = ""
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Operation timeout!"
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = "This is not the leader!"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	leaveGIDs := make([]int, len(args.GIDs))
	copy(leaveGIDs, args.GIDs)
	param := LeaveParam{leaveGIDs}
	op := Op{param, args.SeriesNumber, args.ClientId}
	commitChannel := sm.start("Leave", op)
	select {
	case <-commitChannel:
		reply.Err = ""
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Operation timeout!"
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = "This is not the leader!"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	param := MoveParam{args.Shard, args.GID}
	op := Op{param, args.SeriesNumber, args.ClientId}
	commitChannel := sm.start("Move", op)
	select {
	case <-commitChannel:
		reply.Err = ""
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Operation timeout!"
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = "This is not the leader!"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	param := QueryParam{args.Num}
	op := Op{param, args.SeriesNumber, args.ClientId}
	commitChannel := sm.start("Query", op)
	var config Config
	select {
	case config = <-commitChannel:
		reply.Err = ""
		reply.Config = config
		fmt.Printf("[server %d] config = %v, configVersion = %v, l = %v\n", sm.me, config, param.Num, len(sm.configs)-1)
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Operation timeout!"
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.quit <- 0
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) reshard(config *Config) {
	shardId := 0
	if len(config.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	for {
		gids := []int{}
		for gid, _ := range config.Groups {
			gids = append(gids, gid)
		}
		sort.Ints(gids)
		for _, gid := range gids {
			config.Shards[shardId] = gid
			shardId += 1
			if shardId >= NShards {
				return
			}
		}
	}
}

func copyConfig(num int, shards [NShards]int, groups map[int][]string) Config {
	newGroups := make(map[int][]string)
	for k, v := range groups {
		newGroups[k] = v
	}
	return Config{num, shards, newGroups}
}

func (sm *ShardMaster) execute(op *Op, index int) {
	sm.lock("execute")
	defer sm.unlock("execute")
	if lastExecuteSerialNumber, ok := sm.lastExecuteSerialNumber[op.ClientId]; ok && op.SeriesNumber <= lastExecuteSerialNumber {
		return
	}
	if sm.lastExecuteRaftIndex >= index {
		return
	}
	configVersion := len(sm.configs) - 1
	var config Config
	switch op.Param.(type) {
	case JoinParam:
		param := op.Param.(JoinParam)
		config = sm.configs[configVersion]
		config = copyConfig(config.Num+1, config.Shards, config.Groups)
		for gid, servers := range param.Servers {
			if _, ok := config.Groups[gid]; ok {
				fmt.Printf("[server %d]: GID %v already exists in current config version %v.\n", sm.me, gid, configVersion)
				continue
			}
			config.Groups[gid] = servers
		}
		sm.reshard(&config)
		sm.configs = append(sm.configs, config)
	case LeaveParam:
		param := op.Param.(LeaveParam)
		config = sm.configs[configVersion]
		config = copyConfig(config.Num+1, config.Shards, config.Groups)
		for _, gid := range param.GID {
			if _, ok := config.Groups[gid]; !ok {
				fmt.Printf("[server %d]: Error! GID %v doesn't exist in config version %v.\n", sm.me, gid, configVersion)
			}
			delete(config.Groups, gid)
		}
		sm.reshard(&config)
		sm.configs = append(sm.configs, config)
	case MoveParam:
		param := op.Param.(MoveParam)
		config = sm.configs[configVersion]
		config = copyConfig(config.Num+1, config.Shards, config.Groups)
		config.Shards[param.Shard] = param.GID
		sm.configs = append(sm.configs, config)
	case QueryParam:
		param := op.Param.(QueryParam)
		if param.Num == -1 || param.Num > configVersion {
			param.Num = configVersion
		}
		config = sm.configs[param.Num]
		config = copyConfig(config.Num, config.Shards, config.Groups)
	}
	sm.lastExecuteSerialNumber[op.ClientId] = op.SeriesNumber
	sm.lastExecuteRaftIndex = index
	opkey := OpKey{op.SeriesNumber, op.ClientId}
	if commitChannel, ok := sm.commitChannel[opkey]; ok {
		commitChannel <- config
	}
}

func (sm *ShardMaster) monitorApplyCh(applyCh chan raft.ApplyMsg) {
	for {
		select {
		case msg := <-applyCh:
			switch msg.Command.(type) {
			case Op:
				op := msg.Command.(Op)
				sm.execute(&op, msg.CommandIndex)
			default:
				fmt.Printf("%v doesn't have type Op.\n", msg)
			}
		case <-sm.quit:
			return
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {

	labgob.Register(Op{})
	labgob.Register(JoinParam{})
	labgob.Register(LeaveParam{})
	labgob.Register(QueryParam{})
	labgob.Register(MoveParam{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 10000)
	sm.quit = make(chan int, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.commitChannel = make(map[OpKey]chan Config)
	sm.lastExecuteSerialNumber = make(map[int64]int)

	// Your code here.

	go sm.monitorApplyCh(sm.applyCh)

	return sm
}
