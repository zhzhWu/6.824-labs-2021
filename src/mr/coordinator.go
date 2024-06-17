package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var lock sync.Mutex //用于避免给worker分配任务时发生竞争

type TaskType int  //任务类型， Map or Reduce
type TaskState int //任务的执行状态
type Phase int     //Coordinator 所处阶段 Map or Reduce
const (            //任务类型
	MapTask    TaskType = iota //map任务
	ReduceTask                 //reduce任务
)
const ( //任务执行状态
	Working  TaskState = iota //正在执行
	Waiting                   //等待分配
	Finished                  //已完成
)
const ( //Coordinator 所处阶段
	MapPhase    Phase = iota //Map阶段
	ReducePhase              //Reduce阶段
	AllDone                  //全部任务执行完毕
)

type Task struct {
	TaskId     int       //任务唯一标识
	TaskType   TaskType  //任务类型
	TaskState  TaskState //任务状态
	InputFile  []string  //任务文件名，一般一个Map任务输入一个文件，一个Reduce任务输入多个中间文件
	StartTime  time.Time //任务被分配出去的时间，用于超时重新分配
	ReducerNum int       //记录Reducer的数目，用于Map阶段对key做hash，确定key所对应的中间文件
	ReducerId  int       //记录该RedeceTask对应的	ReducerId
}

type Coordinator struct {
	// Your definitions here.
	CurrentPhase      Phase         //Coordinator当前所处阶段
	TaskIdForGen      int           //给每个任务分配一个任务Id
	TaskMap           map[int]*Task //记录已分配出去的任务信息	TaskId ---> *Task
	MapTaskChannel    chan *Task    //存放待分配的MapTask，收到MapTask请求，则从此处分配MapTask给Worker
	ReduceTaskChannel chan *Task    //存放待分配的ReduceTask，收到ReduceTask请求，则从此处分配ReduceTask给Worker
	MapperNum         int           //Mapper的数目，可用于快速判断MapPhase阶段是否完成
	ReducerNum        int           //记录Reducer的数目，用于Map阶段对key做hash，确定key所对应的中间文件
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) checkMapTaskDone() bool {
	donecnt := 0
	undonecnt := 0
	for _, task := range c.TaskMap {
		if task.TaskType == MapTask {
			if task.TaskState == Finished {
				donecnt++
			} else {
				undonecnt++
			}
		}
	}
	if donecnt == c.MapperNum && undonecnt == 0 {
		return true
	}
	return false
}

func (c *Coordinator) checkReduceTaskDone() bool {
	donecnt := 0
	undonecnt := 0
	for _, task := range c.TaskMap {
		if task.TaskType == ReduceTask {
			if task.TaskState == Finished {
				donecnt++
			} else {
				undonecnt++
			}
		}
	}
	if donecnt == c.ReducerNum && undonecnt == 0 {
		return true
	}
	return false
}

func (c *Coordinator) toNextPhase() {
	switch c.CurrentPhase {
	case MapPhase:
		c.CurrentPhase = ReducePhase
		c.MakeReduceTask()
	case ReducePhase:
		c.CurrentPhase = AllDone
	}
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	//根据当前阶段所处阶段，决定分配任务类型
	switch c.CurrentPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 { //MapTaskChannel中尚有暂未分配的MapTask
				mapTaskPtr := <-c.MapTaskChannel
				if mapTaskPtr.TaskType == MapTask && mapTaskPtr.TaskState == Waiting {
					reply.ReplyFlag = TaskGetted
					reply.Task = *mapTaskPtr
					mapTaskPtr.TaskState = Working
					mapTaskPtr.StartTime = time.Now()
					c.TaskMap[mapTaskPtr.TaskId] = mapTaskPtr
				}
			} else { //MapTaskChannel中的MapTask已被全部分配
				reply.ReplyFlag = WaitPlz
				if c.checkMapTaskDone() {
					c.toNextPhase()
				}
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 { //MapTaskChannel中尚有暂未分配的MapTask
				reduceTaskPtr := <-c.ReduceTaskChannel
				if reduceTaskPtr.TaskType == ReduceTask && reduceTaskPtr.TaskState == Waiting {
					reply.ReplyFlag = TaskGetted
					reply.Task = *reduceTaskPtr
					reduceTaskPtr.TaskState = Working
					reduceTaskPtr.StartTime = time.Now()
					c.TaskMap[reduceTaskPtr.TaskId] = reduceTaskPtr
				}
			} else { //MapTaskChannel中的MapTask已被全部分配
				reply.ReplyFlag = WaitPlz
				if c.checkReduceTaskDone() {
					c.toNextPhase()
				}
			}
		}
	case AllDone:
		{
			reply.ReplyFlag = FinishAndExit
			fmt.Println("All tasks have been finished!")
		}
	default:
		{
			panic("Undefined Coordinator phase!")
		}
	}
	return nil
}

func (c *Coordinator) UpdateTaskState(args *TaskArgs, reply *TaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	taskid := args.TaskId
	c.TaskMap[taskid].TaskState = Finished //更新task的状态
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	lock.Lock()
	defer lock.Unlock()
	if c.CurrentPhase == AllDone {
		ret = true
	}
	return ret
}

func (c *Coordinator) GenerateTaskId() int {
	res := c.TaskIdForGen
	c.TaskIdForGen++
	return res
}

// 为每个文件创建一个待分配的MapTask，
// 并在Coordinator中记录 TaskId ---> *Task 的映射关系
// 并把初始化的未分配的Map任务放入MapTaskChannel
func (c *Coordinator) MakeMapTask(files []string) {
	var taskptr *Task
	for _, filename := range files {
		taskptr = new(Task)
		taskptr.TaskId = c.GenerateTaskId()
		taskptr.TaskType = MapTask
		taskptr.TaskState = Waiting
		taskptr.InputFile = append(taskptr.InputFile, filename)
		taskptr.ReducerNum = c.ReducerNum
		c.MapTaskChannel <- taskptr
		//c.TaskMap[taskptr.TaskId] = taskptr
	}
}

func (c *Coordinator) MakeReduceTask() {
	var taskptr *Task
	rnum := c.ReducerNum
	//Map执行完生成的中间文件存放在当前目录
	dir, _ := os.Getwd()          //获取当前工作目录
	files, err := os.ReadDir(dir) //获取当前工作目录中的所有文件
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < rnum; i++ { //生成ReducerNum个ReduceTask
		taskptr = new(Task)
		taskptr.TaskId = c.GenerateTaskId()
		//中间文件名mr-X-Y, 其中Y为对应Reducer ID
		//所以可以根据文件后缀确定各Reducer的输入文件
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				taskptr.InputFile = append(taskptr.InputFile, file.Name())
			}
		}
		taskptr.ReducerNum = c.ReducerNum
		taskptr.TaskState = Waiting
		taskptr.TaskType = ReduceTask
		taskptr.ReducerId = i
		c.ReduceTaskChannel <- taskptr
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//初始化Coordinator所处阶段、待分配TaskId、Reducer、Mapper数目
	c.CurrentPhase = MapPhase
	c.TaskIdForGen = 0
	c.ReducerNum = nReduce
	c.MapperNum = len(files)
	//初始化TaskMap，用于记录已完成的Map任务和Reduce任务
	c.TaskMap = make(map[int]*Task, c.ReducerNum+c.MapperNum)
	//初始化MapTaskChannel为缓存大小为c.MapperNum的chan *Task
	c.MapTaskChannel = make(chan *Task, c.MapperNum)
	//构建MapTask任务，并存入MapTaskChannel和TaskMap
	c.MakeMapTask(files)
	//初始化ReducerNum为缓存大小为c.ReducerNum的chan *Task
	c.ReduceTaskChannel = make(chan *Task, c.ReducerNum)
	c.server()
	go c.CrashHandle() //探测并处理超时任务
	return &c
}

func (c *Coordinator) CrashHandle() {
	for {
		time.Sleep(2 * time.Second)
		lock.Lock()
		if c.CurrentPhase == AllDone {
			lock.Unlock()
			break
		}
		for _, task := range c.TaskMap {
			//如果该任务已经执行超过10s仍为结束，则更新任务状态为待分配，并放入对应管道
			if task.TaskState == Working && time.Since(task.StartTime) > 10*time.Second {
				task.TaskState = Waiting
				if task.TaskType == MapTask {
					c.MapTaskChannel <- task
				} else if task.TaskType == ReduceTask {
					c.ReduceTaskChannel <- task
				}
				delete(c.TaskMap, task.TaskId)
			}
		}
		lock.Unlock()
	}
}
