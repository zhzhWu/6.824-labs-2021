package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64 //client的唯一标识
	leaderId   int   //该client所认为的当前leader的id
	commandNum int   //该client的每个command都有一个序列号commandNum,自增
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
	ck.clientId = nrand() //为每个client随机生成clientId
	ck.leaderId = -1      //客户端初始化时，不知道谁是leader
	ck.commandNum = 1     //commandNum从1开始递增
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//构造get请求及返回参数
	args := GetArgs{
		Key:        key,
		ClientId:   ck.clientId,
		CommandNum: ck.commandNum,
	}
	serversNum := len(ck.servers)
	for i := 0; ; i = (i + 1) % serversNum {
		if ck.leaderId != -1 { //如果已知leaderid则直接向leader发起请求，否则从0开始逐个尝试
			i = ck.leaderId
		} else {
			time.Sleep(time.Millisecond * 5)
		}
		reply := GetReply{} //在循环内定义，防止收到不同的i的响应
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok { //如果调用失败,则重试
			ck.leaderId = -1
			continue
		} else {
			switch reply.Err {
			case OK: //找到leader并且leader存储有key:value键值对
				ck.commandNum++ //该client的序列号递增
				ck.leaderId = i //记录leaderid
				return reply.Value
			case ErrNoKey:
				ck.commandNum++
				ck.leaderId = i
				return ""
			case ErrWrongLeader:
				ck.leaderId = -1
				continue
			case ErrTimeout:
				ck.leaderId = -1
				continue
			}
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		ClientId:   ck.clientId,
		CommandNum: ck.commandNum,
	}
	serversNum := len(ck.servers)
	for i := 0; ; i = (i + 1) % serversNum {
		if ck.leaderId != -1 {
			i = ck.leaderId
		} else {
			time.Sleep(time.Millisecond * 5)
		}
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.leaderId = -1
			continue
		} else {
			switch reply.Err {
			case OK:
				ck.commandNum++
				ck.leaderId = i
				return
			case ErrNoKey:
				ck.commandNum++
				ck.leaderId = i
				return
			case ErrWrongLeader:
				ck.leaderId = -1
				continue
			case ErrTimeout:
				ck.leaderId = -1
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
