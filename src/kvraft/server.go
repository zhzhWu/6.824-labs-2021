package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 待写入raft log中的command
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId   int64  //客户端标识
	CommandNum int    //序列号
	OpType     string //操作类型，get put append
	Key        string
	Value      string
}

// 每个server记录着其为每个客户端处理的最后一条指令的信息
type Session struct {
	LastCommandNum int    //为该客户端处理的最后一条指令的序列号
	OpType         string //最后一条指令的指令类型
	Response       Reply  //最后一条指令的返回值
}

type Reply struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB                  map[string]string  //存储键值对
	sessions              map[int64]Session  //记录为每个客户端处理的最后一条指令的信息
	notifyMapCh           map[int]chan Reply //kvserver apply到了等待回复的日志则通过chan通知对应的handler方法回复client，key为日志的index
	logLastApplied        int                //此kvserver已应用的最后一条log的index
	passiveSnapshotBefore bool               // 标志着applyMessage上一个从channel中取出的是被动快照并已安装完
}

// raft对command达成共识后通过applyCh通知kvserver应用该command，
// kvserver应用过该command之后通过notifyCh通知执行结果，然后返回给客户端
func (kv *KVServer) createNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	notifyCh := make(chan Reply, 1)
	kv.notifyMapCh[index] = notifyCh
	return notifyCh
}

// kvserver回复客户端后关闭对应index的notifyCh
func (kv *KVServer) CloseNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.notifyMapCh[index]; ok { //如果该channel存在，则将其关闭并从map中删除
		close(kv.notifyMapCh[index])
		delete(kv.notifyMapCh, index)
	}
}

const RespondTimeout = 500 //kvserver回复client的超时时间，单位 ms

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//避免command重复应用，kvserver收到client的请求后先判断是否是已经处理过的请求
	if args.CommandNum < kv.sessions[args.ClientId].LastCommandNum {
		//该client已经收到该请求正确的回复，只是这个重发请求到的太慢了
		kv.mu.Unlock()
		return
	}
	if args.CommandNum == kv.sessions[args.ClientId].LastCommandNum {
		reply.Value = kv.sessions[args.ClientId].Response.Value
		reply.Err = kv.sessions[args.ClientId].Response.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	getOp := Op{
		ClientId:   args.ClientId,
		CommandNum: args.CommandNum,
		OpType:     "Get",
		Key:        args.Key,
	}
	index, _, isLeader := kv.rf.Start(getOp) //把该操作写到raft log
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := kv.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.CloseNotifyCh(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//避免command重复应用，kvserver收到client的请求后先判断是否是已经处理过的请求
	if args.CommandNum < kv.sessions[args.ClientId].LastCommandNum {
		//该client已经收到该请求正确的回复，只是这个重发请求到的太慢了
		kv.mu.Unlock()
		return
	}
	if args.CommandNum == kv.sessions[args.ClientId].LastCommandNum {
		reply.Err = kv.sessions[args.ClientId].Response.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	paOp := Op{
		ClientId:   args.ClientId,
		CommandNum: args.CommandNum,
		OpType:     args.Op,
		Key:        args.Key,
		Value:      args.Value,
	}
	index, _, isleader := kv.rf.Start(paOp)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := kv.createNotifyCh(index) //通过该channel可得知command什么时候执行结束，以及执行结果
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.CloseNotifyCh(index)
}

func (kv *KVServer) applyMessage() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			//应用之前先判断该指令是否已被应用过
			if msg.CommandIndex <= kv.logLastApplied {
				kv.mu.Unlock()
				continue
			}

			// 如果上一个取出的是被动快照且已安装完，则要注意排除“跨快照指令”
			// 因为被动快照安装完后，后续的指令应该从快照结束的index之后紧接着逐个apply
			if kv.passiveSnapshotBefore {
				if msg.CommandIndex-kv.logLastApplied != 1 {
					kv.mu.Unlock()
					continue
				}
				kv.passiveSnapshotBefore = false
			}

			//如果是未应用过的指令，则更新logLastApplied
			kv.logLastApplied = msg.CommandIndex
			op, ok := msg.Command.(Op) //类型断言,把msg.Command转换为Op类型
			if !ok {
				DPrintf("convert fail!\n")
			} else { //断言成功
				reply := Reply{}
				sessionRec, exist := kv.sessions[op.ClientId]
				//再次检查该指令是否已被应用过
				if exist && op.CommandNum <= sessionRec.LastCommandNum {
					if op.CommandNum == sessionRec.LastCommandNum {
						reply = kv.sessions[op.ClientId].Response //如果已经应用过，则直接返回上次的结果
					}
				} else { //如果没有应用过，则把该指令应用到上层状态机kvserver
					switch op.OpType {
					case "Get":
						val, existKey := kv.kvDB[op.Key]
						if !existKey { //如果该key不存在，则返回空字符串
							reply.Err = ErrNoKey
							reply.Value = ""
						} else {
							reply.Err = OK
							reply.Value = val
						}
					case "Put":
						kv.kvDB[op.Key] = op.Value
						reply.Err = OK
					case "Append":
						oldValue, existKey := kv.kvDB[op.Key]
						if !existKey {
							reply.Err = ErrNoKey
							kv.kvDB[op.Key] = op.Value
						} else {
							reply.Err = OK
							kv.kvDB[op.Key] = oldValue + op.Value
						}
					default:
						DPrintf("Unexpected OpType!\n")
					}
					//更新已执行的最后一条指令信息
					session := Session{
						LastCommandNum: op.CommandNum,
						OpType:         op.OpType,
						Response:       reply,
					}
					kv.sessions[op.ClientId] = session
				}
				//检查是否有线程在commandindex channel上等待
				if _, existCh := kv.notifyMapCh[msg.CommandIndex]; existCh {
					//只有leader才需要向客户端返回执行结果，follower只需应用到上层状态机，无需返回
					if cterm, isleader := kv.rf.GetState(); isleader && msg.CommandTerm == cterm {
						kv.notifyMapCh[msg.CommandIndex] <- reply
					}
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			//安装快照之前先在raft层进行判断，只有被接收了的快照才能被安装
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.applySnapshotToSM(msg.Snapshot)
				kv.logLastApplied = msg.SnapshotIndex
				kv.passiveSnapshotBefore = true         // 刚安装完被动快照，提醒下一个从channel中取出的若是指令则注意是否为“跨快照指令”
				kv.rf.SetPassiveSnapshottingFlag(false) //已完成被动快照，所以SetPassiveSnapshottingFlag(false)
			}
			kv.mu.Unlock()
		} else {
			DPrintf("KVServer[%d] get an unexpected ApplyMsg!\n", kv.me)
		}
	}
}

// 把快照应用到上层状态机
func (kv *KVServer) applySnapshotToSM(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDB map[string]string
	var sessions map[int64]Session
	if d.Decode(&kvDB) != nil || d.Decode(&sessions) != nil {
		DPrintf("KVServer %d applySnapshotToSM ERROR!\n", kv.me)
	} else {
		kv.kvDB = kvDB
		kv.sessions = sessions
	}
}

func (kv *KVServer) checkSnapshotNeed() {
	for !kv.killed() {
		var snapshotData []byte
		var snapshotIndex int
		if kv.rf.GetPassiveFlagAndSetActiveFlag() { //如果正在进行被动快照，则放弃本次主动快照检查
			time.Sleep(time.Millisecond * 50)
			continue
		}
		if kv.maxraftstate != -1 && float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) > 0.9 {
			kv.mu.Lock()
			snapshotIndex = kv.logLastApplied
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvDB)
			e.Encode(kv.sessions)
			snapshotData = w.Bytes()
			kv.mu.Unlock()
		}
		if snapshotData != nil {
			kv.rf.Snapshot(snapshotIndex, snapshotData)
		}
		kv.rf.SetActiveSnapshottingFlag(false) // 无论检查完需不需要主动快照都要将主动快照标志修改回false
		time.Sleep(time.Millisecond * 50)      //每隔50ms检查一次是否需要创建快照
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	var lock sync.Mutex
	kv.mu = lock
	kv.dead = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvDB = make(map[string]string)
	kv.sessions = make(map[int64]Session)
	kv.notifyMapCh = make(map[int]chan Reply)
	kv.logLastApplied = 0
	kv.passiveSnapshotBefore = false
	// You may need initialization code here.
	go kv.applyMessage()      //读applyCh，应用日志或快照
	go kv.checkSnapshotNeed() //定期检查是否需要快照
	return kv
}
