package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ReqTaskReplyFlag int

const (
	TaskGetted    ReqTaskReplyFlag = iota //Coordinator给worker分配任务成功
	WaitPlz                               //当前阶段暂时没有尚未分配的任务
	FinishAndExit                         //MapReduce工作全部完成，worker准备退出
)

type TaskArgs struct {
	TaskId int
}

type TaskReply struct {
	ReplyFlag ReqTaskReplyFlag //任务请求返回标志
	Task      Task             //返回任务
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
