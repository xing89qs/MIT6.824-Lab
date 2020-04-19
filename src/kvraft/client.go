package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64
	seriesNumber int
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

	// You will have to modify this function.
	for {
		time.Sleep(100 * time.Millisecond)
		lastLeader := ck.lastLeader
		args := GetArgs{key}
		reply := GetReply{}
		if lastLeader != nil {
			lastLeader.Call("KVServer.Get", &args, &reply)
		}
		if reply.WrongLeader {
			for _, server := range ck.servers {
				time.Sleep(time.Duration(100) * time.Millisecond)
				reply = GetReply{}
				server.Call("KVServer.Get", &args, &reply)
				if !reply.WrongLeader {
					ck.lastLeader = server
					break
				}
			}
		}
		if reply.Err != "" {
			DPrintf("Called for key %s, but err: %s", key, reply.Err)
			continue
		}
		DPrintf("Get value %s for key %s", reply.Value, key)
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
	args := PutAppendArgs{key, value, op, ck.seriesNumber, ck.clientId}
	ck.seriesNumber += 1
	reply := PutAppendReply{WrongLeader: true}
	for {
		time.Sleep(100 * time.Millisecond)
		lastLeader := ck.lastLeader
		if lastLeader != nil {
			lastLeader.Call("KVServer.PutAppend", &args, &reply)
		}
		if reply.WrongLeader {
			for _, server := range ck.servers {
				time.Sleep(time.Duration(100) * time.Millisecond)
				reply = PutAppendReply{}
				server.Call("KVServer.PutAppend", &args, &reply)
				if !reply.WrongLeader {
					ck.lastLeader = server
					break
				}
			}
		}
		if reply.Err != "" {
			DPrintf("Client Error: %v", reply.Err)
			continue
		}
		DPrintf("Command %s %s:%s submitted.", op, key, value)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
