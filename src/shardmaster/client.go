package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader *labrpc.ClientEnd

	clientId     int64
	serialNumber int
	mu           sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.lastLeader = nil
	ck.clientId = nrand()
	ck.serialNumber = 0
	return ck
}

func (ck *Clerk) lock(s string) {
	ck.mu.Lock()
	// fmt.Printf("[client %d] Lock %s\n", ck.clientId, s)
}

func (ck *Clerk) unlock(s string) {
	// fmt.Printf("[client %d] Unlock %s\n", ck.clientId, s)
	ck.mu.Unlock()
}

func (ck *Clerk) reorderServer() []*labrpc.ClientEnd {
	if ck.lastLeader == nil {
		return ck.servers
	}
	servers := make([]*labrpc.ClientEnd, len(ck.servers))
	servers = append(servers, ck.lastLeader)
	for _, svr := range ck.servers {
		servers = append(servers, svr)
	}
	return servers
}

func (ck *Clerk) Query(num int) Config {
	ck.lock("Query")
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	ck.serialNumber += 1
	args.SeriesNumber = ck.serialNumber
	ck.unlock("Query")
	for {
		servers := ck.reorderServer()
		// try each known server.
		for _, srv := range servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.lock("Join")
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	ck.serialNumber += 1
	args.SeriesNumber = ck.serialNumber
	ck.unlock("Join")

	for {
		servers := ck.reorderServer()
		// try each known server.
		for _, srv := range servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.lock("Leave")
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	ck.serialNumber += 1
	args.SeriesNumber = ck.serialNumber
	ck.unlock("Leave")
	for {
		servers := ck.reorderServer()
		// try each known server.
		for _, srv := range servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.lock("Move")
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	ck.serialNumber += 1
	args.SeriesNumber = ck.serialNumber
	ck.unlock("Move")

	for {
		servers := ck.reorderServer()
		// try each known server.
		for _, srv := range servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
