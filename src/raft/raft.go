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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

const (
	HeartbeatMs  = 100 // leader 100Ms 发起一次心跳
	MinTimeoutMs = 250 // 最小超时时间
	MaxTimeOutMs = 400 // 最大超时时间
)

type LogEntry struct {
	Command interface{} //客户端指令
	Term    int         //此日志条目所属Term
	Index   int         //此日志条目的index
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//所有服务器上的持久状态
	currentTerm int        //当前服务器可见的最新任期，服务器启动时初始化为0,递增
	votedFor    int        //收到当前服务器在当前任期选票的候选人candidateId
	log         []LogEntry //此Server所包含的所有日志，第一条有效日志的索引为1,log[0]被无效日志条目所填充

	currentState ServerState   //当前服务器在当前任期内的角色状态
	leaderId     int           //当前任期内当前服务器可见的leaderId，可用于重定向客户端请求
	hbInterval   time.Duration //心跳间隔、每秒心跳不超过10次
	timeout      time.Time     //记录当前服务器的超时时刻（长期未接受到心跳信号、选举超时）	超时时间间隔设置为250-400ms

	//所有server的易失状态
	commitIndex int //当前服务器已知的已被提交的最后一个日志条目的Index
	lastApplied int //每个Raft已经应用到上层状态机的日志的最高索引，初始化为零，单调递增

	//Leader的易失状态，每次当选时重新初始化
	nextIndex  []int //leader记录待发送给每个server的下一日志条目的index,每次当选都重新初始化为leader最后一条日志的index + 1
	matchIndex []int //leader记录和每个server已匹配的最后一条日志的index，每次当选都初始化为0

	applyCh chan ApplyMsg //通过此channel模拟把已提交的日志提交到状态机

	lastIncludedIndex int //快照中所包含的最后一个logEntry的Index
	lastIncludedTerm  int //快照中所包含的最后一个logEntry的Term

	//主动快照和被动快照冲突，不可同时进行，避免主、被动快照重叠应用导致上层状态机状态与下层raft日志不一致
	activeSnapshotting  bool //标记当前server是否正在进行主动快照
	passiveSnapshotting bool //标记当前server是否正在进行主动快照
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.currentState == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// 每次需持久化的数据被修改后都需立即调用rf.persist()进行持久化
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 服务器（重新）启动时，需加载之前持久化的数据
// restore previously persisted state.
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Raft server %d readPersist ERROR!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// 崩溃后从快照恢复
func (rf *Raft) recoverFromSnap(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { //如果没有快照则直接返回
		return
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapshotTerm:  rf.lastIncludedTerm,
		Snapshot:      snapshot,
	}
	go func(msg ApplyMsg) { rf.applyCh <- msg }(snapshotMsg)
	DPrintf("Server %d recover from crash and send SnapshotMsg to ApplyCh.\n", rf.me)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.lastIncludedIndex || lastIncludedIndex <= rf.lastApplied || lastIncludedIndex <= rf.commitIndex {
		return false
	}
	var newLog []LogEntry
	rf.lastApplied = lastIncludedIndex
	//如果该server上有比leader传来的快照更新的日志，则做完快照后，快照之前的日志仍应正常保留
	if lastIncludedIndex < rf.log[len(rf.log)-1].Index {
		//如果本地日志和leader传来的快照有冲突，则清空本地日志
		if rf.log[lastIncludedIndex-rf.lastIncludedIndex].Term != lastIncludedTerm ||
			rf.log[lastIncludedIndex-rf.lastIncludedIndex].Index != lastIncludedIndex {
			newLog = []LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex}}
			rf.commitIndex = lastIncludedIndex
		} else {
			newLog = []LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex}}
			newLog = append(newLog, rf.log[lastIncludedIndex-rf.lastIncludedIndex+1:]...)
			if lastIncludedIndex > rf.commitIndex {
				rf.commitIndex = lastIncludedIndex
			}
		}
	} else {
		newLog = []LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex}}
		rf.commitIndex = lastIncludedIndex
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.log = newLog
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果主动快照的index不大于lastIncludedIndex，则表明此次快照是重复或更旧的，则直接返回
	if index <= rf.lastIncludedIndex {
		DPrintf("Server %d refuse this positive snapshot.\n", rf.me)
		rf.mu.Unlock()
		return
	}
	DPrintf("Server %d start to positively snapshot(rf.lastIncluded=%v, snapshotIndex=%v).\n", rf.me, rf.lastIncludedIndex, index)
	//丢弃Index及其之前的LogEntries
	//第一条(索引为0)的logEntry仍为占位日志(无效日志)，其中存储着最近一次快照包含的最后一个logEntry的term和index
	var newlog = []LogEntry{{Term: rf.log[index-rf.lastIncludedIndex].Term, Index: index}}
	newlog = append(newlog, rf.log[index-rf.lastIncludedIndex+1:]...)
	rf.log = newlog
	rf.lastIncludedIndex = newlog[0].Index
	rf.lastIncludedTerm = newlog[0].Term
	//更新commitIndex和lastApplied
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	//持久化
	rf.persist()
	state := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// leader发送SnapShot信息给落后的Follower
// idx是要发送给的follower的序号
func (rf *Raft) LeaderSendSnapshot(idx int, snapshot []byte) {
	rf.mu.Lock()
	cterm := rf.currentTerm

	//如果leader被kill或状态发生了变化，则直接返回
	if rf.killed() || rf.currentState != Leader {
		rf.mu.Unlock()
		return
	}
	//构造InstallSnapshot RPC请求参数
	args := InstallSnapshotArgs{
		Term:              cterm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		SnapshotData:      snapshot,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	DPrintf("Leader %d sends InstallSnapshot RPC(term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d) to server %d...\n",
		rf.me, cterm, args.LastIncludedIndex, args.LastIncludedTerm, idx)
	//向follower发送InstallSnapshot RPC，让其安装新的快照
	ok := rf.peers[idx].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		DPrintf("Leader %d calls server %d for InstallSnapshot failed!\n", rf.me, idx)
		// 如果由于网络原因或者follower故障等收不到RPC回复
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果leader状态发生变化，或收到旧任期内的RPC回复，则直接返回
	if rf.currentState != Leader || rf.currentTerm != args.Term {
		return
	}
	//如果leader发现自己任期落后，则更新任期并转为follower
	if rf.currentTerm < reply.Term {
		rf.votedFor = -1
		rf.currentState = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	//如果follower接受了leader的快照，则更新follower的matchIndex等
	if reply.Accept && rf.matchIndex[idx] < args.LastIncludedIndex {
		rf.matchIndex[idx] = args.LastIncludedIndex
		rf.nextIndex[idx] = rf.matchIndex[idx] + 1
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 接受leader的被动快照前先检查给server是否正在进行主动快照，若是则本次被动快照取消
	// 避免主、被动快照重叠应用导致上层kvserver状态与下层raft日志不一致
	if args.Term < rf.currentTerm || rf.activeSnapshotting {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	//如果follower所处任期落后，则更新所处任期
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.currentState = Follower
	rf.leaderId = args.LeaderId
	rf.resetTimeout()

	//通知上层状态机安装快照
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.SnapshotData,
	}
	go func(msg ApplyMsg) { rf.applyCh <- msg }(snapshotMsg)

	//DPrintf("Server %d accept the snapshot from leader(lastIncludedIndex=%v, lastIncludedTerm=%v).\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	reply.Term = rf.currentTerm
	reply.Accept = true
	return
}

// leader向follower发送快照的InstallSnapshot RPC请求结构
type InstallSnapshotArgs struct {
	Term              int    //leader的任期
	LeaderId          int    //LeaderID
	LastIncludedIndex int    //本次所发快照中包含的最后一个logEntry的Index
	LastIncludedTerm  int    //本次所发快照中包含的最后一个logEntry的Term
	SnapshotData      []byte //快照数据
}

// leader向follower发送快照的InstallSnapshot RPC返回结构
type InstallSnapshotReply struct {
	Term   int  //follower所属任期
	Accept bool //follower是否接收这个快照
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate的当前任期，即在该任期内发起选举
	CandidateId  int //请求投票的CandidateId，即其在peers数组中的索引
	LastLogIndex int //请求投票的Candidate最后一条日志的Index	（用于确保安全性的选举限制）
	LastLogTerm  int //请求投票的Candidate最后一条日志所属的Term		（用于确保安全性的选举限制）
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前服务器的currentTerm，如果候选人发现自己发起选举的任期小于peers返回的任期，则退回follower
	VoteGranted bool //当候选人收到该peer的选票时为true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Figure2 RequestVote RPC : Receiver implementation 1
	//如果candidate的任期小于当前服务器的任期，则告诉candidate当前任期，让其退回follower状态
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Figure2 Rules for Servers : All Servers
	//如果RPC请求或回复中的Term > 当前服务器的currentTerm，则 set currentTerm = Term，并切换状态为follower
	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.leaderId = -1
		rf.currentState = Follower
		rf.currentTerm = args.Term
		rf.persist()
	}

	// Figure2 RequestVote RPC : Receiver implementation 2
	//candidate的日志至少要和待投票的server（即当前server）的日志一样新
	//如果最后一个日志条目的Term不同，则term大的比Term小的新
	//如果最后一个日志条目的Term相同，则哪个日志更长，哪个就更新
	uptodate := false
	voterLastLog := rf.log[len(rf.log)-1]
	if (args.LastLogTerm > voterLastLog.Term) || (args.LastLogTerm == voterLastLog.Term && args.LastLogIndex >= voterLastLog.Index) {
		uptodate = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		rf.votedFor = args.CandidateId
		rf.leaderId = -1
		reply.VoteGranted = true
		rf.persist()
		rf.resetTimeout()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return 0, -1, false
	}
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	index = rf.log[len(rf.log)-1].Index + 1 //当日志被提交时，会被追加到当前索引
	if isLeader == false {
		rf.mu.Unlock()
		return index, term, false
	}
	//如果是Leader，则向其他server复制日志
	newLogEntry := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, newLogEntry) //leader首先把新日志追加到自己的log中去
	rf.persist()
	rf.mu.Unlock()
	go rf.LeaderAppendEntries() //然后把其追加到其他服务器上
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Convert2Candidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderId = -1 //当前服务器发起选举，则对当前服务器来说，当前任期内的leaderId待定
	rf.persist()
}

func (rf *Raft) Convert2Leader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentState = Leader
	//rf.votedFor = -1
	rf.leaderId = rf.me

	//每次当选初始化nextIndex,matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
		rf.matchIndex[i] = 0
	}
	rf.timeout = time.Now() //把超时时间重置为now，方便rf.ticker快速检测到leader超时，并发送心跳包
}

func (rf *Raft) RunForElection() {
	rf.Convert2Candidate()
	rf.mu.Lock()
	cterm := rf.currentTerm //确保在当前服务器在相同的任期内向peers发送投票请求
	rf.mu.Unlock()
	//记录已获得的选票数以及选举是否完成
	votes := 1                    //得票数（含自己投给自己的选票）
	finished := 1                 //收到的请求投票回复数
	var voteMu sync.Mutex         //用于保护votes和finished
	cond := sync.NewCond(&voteMu) //可用于阻塞等待请求投票RPC的返回结果
	//candidate向除自己以外的其他server发送请求投票RPC
	for i, _ := range rf.peers { //i表示当前服务器向第i个peer发送投票请求RPC
		if rf.killed() { //如果在竞选过程中candidate被kill，则直接退出
			return
		}
		rf.mu.Lock()
		if rf.currentState != Candidate { //如果竞选过程中发现自己已不是candidate（可能已选出其他leader，自己退回follower），则结束竞选
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if i == rf.me { //自己已把选票投给自己
			continue
		}
		go func(idx int) {
			//构造请求投票RPC参数
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         cterm, //确保在同一个任期内peers发起投票请求
				CandidateId:  rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(idx, &args, &reply) // candidate向server i 发送请求投票RPC
			if !ok {
				DPrintf("Candidate %d call server %d for RequestVote failed!\n", rf.me, idx)
			}

			rf.mu.Lock()
			//比较当前任期和原始RPC中发送的任期，如果不同，则放弃回复并返回
			if rf.currentState != Candidate || rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			//如果当前任期比其他server的任期小，则退回follower状态，并更新自己所属的任期
			if rf.currentTerm < reply.Term {
				rf.votedFor = -1
				rf.currentState = Follower
				rf.currentTerm = reply.Term
				rf.resetTimeout() //由Candidate变回Follower，重置选举超时时间
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			//检查是否获得选票
			voteFlag := reply.VoteGranted
			voteMu.Lock()
			if voteFlag {
				votes++ //成功获得选票
			}
			finished++ //每收到一个返回，finished递增
			voteMu.Unlock()
			cond.Broadcast()
		}(i)
	}
	sumNum := len(rf.peers)     //集群中总的server数
	majorityNum := sumNum/2 + 1 //集群中的最少多数节点
	voteMu.Lock()
	for votes < majorityNum && finished != sumNum {
		cond.Wait() //票数尚不足当选且仍有RPC未返回时，在此阻塞等待请求投票RPC返回
		rf.mu.Lock()
		stillCandidate := (rf.currentState == Candidate)
		rf.mu.Unlock()
		if !stillCandidate {
			voteMu.Unlock()
			return
		}
	}
	if votes >= majorityNum {
		rf.Convert2Leader() //如果胜选，则变为Leader
	} else {
		DPrintf("Candidate %d failed in the election and continued to wait...\n", rf.me)
	}
	voteMu.Unlock()
}

// 追加日志条目RPC的发送参数
type AppendEntriesArgs struct {
	Term         int        //leader的任期
	LeaderId     int        //leader的Id，便于follower收到客户端请求时做重定向
	PrevLogIndex int        //待追加日志的前一条日志的Index
	PrevLogTerm  int        //待追加日志的前一条日志的Term
	Entries      []LogEntry //待追加日志条目的内容，可一次追加多条，当发送心跳包时此项为空
	LeaderCommit int        //leader已提交的最后一条日志的Index，即leader的commitIndex
}

// 追加日志条目RPC的返回参数
type AppendEntriesReply struct {
	Term          int  //follower的currentTerm,leader可根据此判断自己任期是否落后
	Success       bool //如果follower包含有匹配leader的PrevLogIndex和PrevLogTerm的日志条目，则返回true
	ConflictIndex int  //当待追加日志条目的前一日志条目不匹配时，用于加速日志回溯
	ConflictTerm  int
}

// AppendEntries RPC handler
// 其他servers收到leader的追加日志rpc或心跳包后进行逻辑处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//检查日志是匹配的才接收
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果接收到的AppendEntries RPC的args的Term小于当前服务器的currentTerm
	//则拒绝追加，并返回自己当前的任期，
	if args.Term < rf.currentTerm { //由于此AppendEntries RPC是由过期的Term产生的，所以收到该RPC后不能重置定时器
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//Figure2 Rules for Servers : All Servers 2
	//如果当前服务器的任期小于RPC请求或响应的任期，则更新当前服务其的任期
	if args.Term > rf.currentTerm {
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}
	// 这里实现了candidate或follower在收到leader的心跳包或日志追加RPC后重置计时器并维持follower状态
	rf.currentState = Follower  // 变回或维持Follower
	rf.leaderId = args.LeaderId // 将rpc携带的leaderId设为自己的leaderId，记录最近的leader（client寻找leader失败时用到）
	DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
		rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)
	rf.resetTimeout()

	//如果待追加的logEntries中有部分或全部已在本地快照中，则进行特殊处理
	if args.PrevLogIndex < rf.lastIncludedIndex {
		//如果待追加的logEntries过于陈旧，则不进行追加
		if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index <= rf.lastIncludedIndex {
			reply.Success = true
			reply.Term = rf.currentTerm
			return
		} else {
			args.Entries = args.Entries[rf.lastIncludedIndex-args.PrevLogIndex:]
			args.PrevLogIndex = rf.lastIncludedIndex
			args.Term = rf.lastIncludedTerm
		}
	}

	// Figure2 AppendEntries RPC : Receiver implementation 2,3
	//如果当前服务器在args.PrevLogIndex处没有日志条目，或虽有日志条目但Term不匹配
	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex || rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		if rf.log[len(rf.log)-1].Index < args.PrevLogIndex { //当前服务器在args.PrevLogIndex处没有日志条目
			reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
			reply.ConflictTerm = -1
		} else { //虽有日志，但任期不匹配
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
			i := args.PrevLogIndex - 1 - rf.lastIncludedIndex
			for i >= 0 && rf.log[i].Term == reply.ConflictTerm {
				i--
			}
			reply.ConflictIndex = i + 1 + rf.lastIncludedIndex //回退到冲突任期内的第一个日志条目
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else { //前一条日志条目匹配成功
		//如果前一条日志条目匹配成功，则判断该AppendEntries RPC中是否包含当前服务器中没有的日志条目
		//如果包含，则删除该服务器prevLogIndex之后的所有日志条目，并追加该AppendEntries RPC中的所有条目
		//如果不包含，则忽略该AppendEntries RPC（可能是一个过期的RPC），
		//如果响应一个过期的RPC，则需截断日志，而截断日志意味着“收回”我们可能已经告诉领导者我们在日志中拥有的条目。
		//如果当前服务器上存储的日志条目数（包含索引为0的无效日志）len(rf.log)
		//小于args.PrevLogIndex+1+len(args.Entries)（这里的1即表示索引为1的无效日志）
		//则该AppendEntries RPC中一定包含当前服务器中没有的日志条目
		//如果该AppendEntries RPC中的最后一条日志条目的任期或索引和当前服务器对应位置日志条目的任期或索引不同
		//则也表明该AppendEntries RPC中一定包含当前服务器中没有的日志条目
		//如果不是心跳包，且满足上述条件

		//找到不匹配的日志条目的索引
		misMatchIndex := -1
		for i, entry := range args.Entries {
			if args.PrevLogIndex+1+i > rf.log[len(rf.log)-1].Index || rf.log[args.PrevLogIndex-rf.lastIncludedIndex+1+i].Term != entry.Term || rf.log[args.PrevLogIndex-rf.lastIncludedIndex+1+i].Index != entry.Index {
				misMatchIndex = args.PrevLogIndex + 1 + i
				break
			}
		}
		//如果存在不匹配的日志条目，则覆盖其以及其之后的日志条目
		if misMatchIndex != -1 {
			logCopy := rf.log[:misMatchIndex-rf.lastIncludedIndex]
			logCopy = append(logCopy, args.Entries[misMatchIndex-args.PrevLogIndex-1:]...)
			rf.log = logCopy
			//DPrintf("Server %d AppendEntries (term:%d, Entries len:%d, PrevLogIndex:%d) from Leader %d.\n", rf.me, args.Term, len(args.Entries), args.PrevLogIndex, args.LeaderId)
		}

		rf.persist()

		//Figure2 AppendEntries RPC : Receiver implementation 5
		//如果当前服务器的commitIndex小于收到RPC中的leaderCommit，则 set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.log[len(rf.log)-1].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log[len(rf.log)-1].Index
			}
		}
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) LeaderAppendEntries() {
	rf.mu.Lock()
	cterm := rf.currentTerm
	//由于之后要根据matchIndex是否达成共识（即复制到大多数server）来更新commitIndex，所以这里先更新leader的matchIndex和nextIndex
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index //部分Server的日志条目和leader的相匹配，但可能为达到大多数，所以不能更新commitIndex
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	rf.mu.Unlock()
	//向除自己以外的Server发送AppendEntries RPC
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//利用goroutine并发发送AppendEntries RPC
		go func(idx int) {
			if rf.killed() { //如果发送AppendEntries RPC的过程中leader被kill，则直接结束
				return
			}
			rf.mu.Lock()
			if rf.currentState != Leader {
				rf.mu.Unlock()
				return
			}
			appendLogs := []LogEntry{}   //如果发送心跳包，则appendLogs为空
			nextIdx := rf.nextIndex[idx] //待发送日志条目的Index

			//如果要追加的日志已经被截断了，则向该follower发送快照
			if nextIdx <= rf.lastIncludedIndex { // lab2D
				go rf.LeaderSendSnapshot(idx, rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				return
			}

			//Figure 2 : Rules for Servers : Leaders
			if rf.log[len(rf.log)-1].Index >= nextIdx {
				appendLogs = make([]LogEntry, rf.log[len(rf.log)-1].Index-nextIdx+1) //为appendLogs分配足够的空间
				copy(appendLogs, rf.log[nextIdx-rf.lastIncludedIndex:])              //把leader的log中nextIdx及之后的日志条目拷贝到appendLogs，准备发送
				//appendLogs = append(appendLogs, rf.log[nextIdx:]...) //把leader的log中nextIdx及之后的日志条目拷贝到appendLogs，准备发送
			}
			prevLog := rf.log[nextIdx-rf.lastIncludedIndex-1] // preLog是leader要发给server idx的日志条目的前一个日志条目
			args := AppendEntriesArgs{
				Term:         cterm,
				LeaderId:     rf.me,
				PrevLogTerm:  prevLog.Term,
				PrevLogIndex: prevLog.Index,
				Entries:      appendLogs,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			DPrintf("Leader %d sends AppendEntries RPC(term:%d, Entries len:%d) to server %d...\n", rf.me, cterm, len(args.Entries), idx)
			// 注意传的是args和reply的地址而不是结构体本身！
			ok := rf.peers[idx].Call("Raft.AppendEntries", &args, &reply) // leader向 server i 发送AppendEntries RPC
			if !ok {
				// 如果由于网络原因或者follower故障等收不到RPC回复（不是follower将回复设为false）
				// 则leader无限期重复发送同样的RPC（nextIndex不前移），等到下次心跳时间到了后再发送
				DPrintf("Leader %d calls server %d for AppendEntries or Heartbeat failed!\n", rf.me, idx)
				return
			}
			//如果leader收到比自己任期更大的follower的回复，则leader更新自己的状态为follower
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//如果自己已不是Leader或收到其他任期的RPC回复，则立即返回
			if rf.currentState != Leader || rf.currentTerm != args.Term {
				return
			}
			if rf.currentTerm < reply.Term {
				rf.votedFor = -1
				rf.leaderId = -1
				rf.currentState = Follower
				rf.currentTerm = reply.Term
				rf.persist()
				rf.resetTimeout() //由Leader变成Follower，重置选举超时时间
				return
			}
			//判断日志是否追加成功
			//AppendEntries有两种情况返回false，第一种是leader的任期落后了，这种情况在上面已处理
			//第二种情况，prevlog不匹配，在此进行处理
			if reply.Success == false {
				possibleNextIdx := 0           //可能的nextIndex[idx]
				if reply.ConflictIndex == -1 { //follower的日志中没有Index为prevLogIndex的日志条目
					possibleNextIdx = reply.ConflictIndex
				} else {
					foundConflictTerm := false
					k := len(rf.log) - 1
					for ; k > 0; k-- {
						if rf.log[k].Term == reply.ConflictTerm {
							foundConflictTerm = true
							break
						}
					}
					if foundConflictTerm {
						possibleNextIdx = rf.log[k+1].Index // 如果找到了对应的Term，则下次发送该Term内的最后一个日志条目
					} else {
						possibleNextIdx = reply.ConflictIndex
					}
				}
				if possibleNextIdx < rf.nextIndex[idx] && possibleNextIdx > rf.matchIndex[idx] {
					rf.nextIndex[idx] = possibleNextIdx
				} else { //否则，把该RPC回复视为过时并丢弃
					return
				}
			} else { //如果向follower追加日志成功
				//更新nextIndex和matchIndex
				possibleMatchIdx := args.PrevLogIndex + len(args.Entries)
				if possibleMatchIdx > rf.matchIndex[idx] {
					rf.matchIndex[idx] = possibleMatchIdx
				}
				rf.nextIndex[idx] = rf.matchIndex[idx] + 1
				sortMatchIndex := make([]int, len(rf.peers))
				copy(sortMatchIndex, rf.matchIndex)
				sort.Ints(sortMatchIndex)
				maxN := sortMatchIndex[(len(sortMatchIndex)-1)/2] //找到所有已复制到大多数节点的日志条目的最大索引
				for N := maxN; N > rf.commitIndex; N-- {
					//Figure2 Rules for Servers : Leaders 4
					//只提交当前任期内的日志
					if rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
						rf.commitIndex = N
						DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, N)
						break
					}
				}
			}
		}(i)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		tout := rf.timeout    //当前服务器超时时刻
		cs := rf.currentState //当前服务器的角色
		rf.mu.Unlock()
		if time.Now().After(tout) {
			switch cs {
			case Follower:
				rf.resetTimeout() //重置超时时间
				DPrintf("Follower Server %v RunForElection\n", rf.me)
				go rf.RunForElection() //发起选举
			case Candidate:
				rf.resetTimeout() //重置超时时间
				DPrintf("Candidate Server %v RunForElection\n", rf.me)
				go rf.RunForElection() //重新发起选举
			case Leader:
				rf.resetTimeoutForLeader()
				DPrintf("Leader Server %v AppendEntries\n", rf.me)
				go rf.LeaderAppendEntries()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) resetTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(MinTimeoutMs+r.Intn(MaxTimeOutMs-MinTimeoutMs+1)))
}

func (rf *Raft) resetTimeoutForLeader() {
	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(100)) //Leader的超时时间设为100ms，如超时，则发送心跳RPC
}

// Lab2B	把每个Raft上已提交但为应用的日志，应用到上层状态机上(可以是kv服务器)，这里只是传入applyCh
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		var applyMsg ApplyMsg
		needApply := false
		//如果有已提交但未应用的日志
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex { //如果命令执行结果已在本地快照中，则不需要再次应用该命令
				rf.lastApplied = rf.lastIncludedIndex
				rf.mu.Unlock()
				continue
			}
			applyMsg = ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				Command:       rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.lastApplied, //rf.log[rf.lastApplied].Index
			}
			needApply = true
		}
		rf.mu.Unlock()
		//虽然此处已经释放了锁，但仍可保证Command按照rf.log中的顺序被应用到上层状态机
		//因为每个raft只有这一个applier goroutine会向本Raft的rf.applyCh中写入数据
		//因此即使释放锁之后立马发生goroutine的调度，也不会有其他的Command被乱序的写入本Raft的rf.applyCh中
		//只有等该applier goroutine再次获取到CPU使用权时，继续按序把调度前为来得及写入本Raft的rf.applyCh中的Command写入该Channel中
		if needApply {
			rf.applyCh <- applyMsg
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	var lock sync.Mutex
	rf.mu = lock
	rf.currentTerm = 0         //把当前任期初始化为0
	rf.currentState = Follower //服务器初始状态为Follower
	rf.votedFor = -1           //初始投票对象为-1，无效
	rf.leaderId = -1           //初始leaderId为-1,无效
	//有效任期从0开始，有效日志Index从1开始，所以在rf.log索引为0的位置填充一条无效日志
	//让日志Index和其在rf.log中的下标相对应
	rf.log = []LogEntry{{Term: -1, Index: 0}}
	rf.hbInterval = HeartbeatMs * time.Millisecond
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.resetTimeout() //重置超时时间
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	rf.activeSnapshotting = false
	rf.passiveSnapshotting = false

	// initialize from state persisted before a crash
	//崩溃恢复
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnap(persister.ReadSnapshot()) //加载快照
	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()
	DPrintf("Server %v (Re)Start and lastIncludedTerm = %v, lastIncludedIndex = %v\n", rf.me, rf.lastIncludedTerm, rf.lastIncludedIndex)
	//另起一个goroutine把已提交但未应用的logEntry应用到上层状态机
	go rf.applier()
	return rf
}
