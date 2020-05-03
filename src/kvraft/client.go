package kvraft

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64
	seriesNumber int
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
	// You'll have to add code here.
	ck.lastLeader = nil
	ck.clientId = nrand()
	ck.seriesNumber = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	DPrintf("Client: Request command %v, key=%s, seriesNumber=%v, clientId=%v", "Get", key, ck.seriesNumber, ck.clientId)
	args := GetArgs{key, ck.seriesNumber, ck.clientId}
	ck.seriesNumber += 1
	ck.mu.Unlock()
	reply := GetReply{WrongLeader: true}
	// You will have to modify this function.
	for {
		time.Sleep(100 * time.Millisecond)
		lastLeader := ck.lastLeader
		reply.Err = ""
		reply.WrongLeader = true
		if lastLeader != nil {
			ok := lastLeader.Call("KVServer.Get", &args, &reply)
			if !ok {
				reply.Err = "RPC timeout"
				reply.WrongLeader = true
			}
		}
		if reply.WrongLeader {
			for _, server := range ck.servers {
				time.Sleep(time.Duration(100) * time.Millisecond)
				reply = GetReply{}
				ok := server.Call("KVServer.Get", &args, &reply)
				if !ok {
					reply.Err = "RPC timeout"
				} else if !reply.WrongLeader {
					ck.mu.Lock()
					ck.lastLeader = server
					ck.mu.Unlock()
					break
				}
			}
		}
		if reply.Err != "" {
			DPrintf("Called for key %s, but err: %s", key, reply.Err)
			continue
		}
		DPrintf("Client: Get value %s for key %s", reply.Value, key)
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	DPrintf("Client: Request command %v, %v:%v, seriesNumber=%v, clientId=%v", op, key, value, ck.seriesNumber, ck.clientId)
	args := PutAppendArgs{key, value, op, ck.seriesNumber, ck.clientId}
	ck.seriesNumber += 1
	ck.mu.Unlock()
	reply := PutAppendReply{WrongLeader: true}
	for {
		time.Sleep(100 * time.Millisecond)
		lastLeader := ck.lastLeader
		reply.Err = ""
		reply.WrongLeader = true
		if lastLeader != nil {
			ok := lastLeader.Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				reply.Err = "RPC timeout"
				reply.WrongLeader = true
			}
		}
		if reply.WrongLeader {
			for _, server := range ck.servers {
				time.Sleep(time.Duration(100) * time.Millisecond)
				reply = PutAppendReply{}
				ok := server.Call("KVServer.PutAppend", &args, &reply)
				if !ok {
					reply.Err = "RPC timeout"
				} else if !reply.WrongLeader {
					ck.mu.Lock()
					ck.lastLeader = server
					ck.mu.Unlock()
					break
				}
			}
		}
		if reply.Err != "" {
			DPrintf("Client Error: %v", reply.Err)
			continue
		}
		DPrintf("Client: Command %s %s:%s submitted.", op, key, value)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	// DPrintf("Client: Request command %v, %v:%v, seriesNumber=%v, clientId=%v", "put", key, value, ck.seriesNumber, ck.clientId)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
