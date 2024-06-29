package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId   int64 //用于标识是哪个客户端发来的请求
	CommandNum int   //该请求在对应客户端中的序列号，避免重复执行
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId   int64 //用于标识是哪个客户端发来的get请求
	CommandNum int   //该get请求在对应客户端中的序列号，避免重复执行
}

type GetReply struct {
	Err   Err
	Value string
}
