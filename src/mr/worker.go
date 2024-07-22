package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 定义序列类型，为KeyValue序列按Key进行排序
type OrderKey []KeyValue

func (a OrderKey) Len() int           { return len(a) }
func (a OrderKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a OrderKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RequestTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok == false {
		fmt.Println("RPC Error: Coordinator.AssignTask failed!!!")
	}
	return reply
}

func FinishTaskAndReport(TaskId int) {
	args := TaskArgs{TaskId}
	reply := TaskReply{}
	ok := call("Coordinator.UpdateTaskState", &args, &reply)
	if ok == false {
		fmt.Println("RPC Error: Coordinator.UpdateTaskState failed!!!")
	}
}

// 读取MapTask文件中的内容，交给mapf处理
func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{} //记录mapf处理后的中间结果
	//一般一个MapTask只有一个输入文件
	for _, filename := range task.InputFile {
		file, err := os.Open(filename) //打开文件
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file) //读取文件中的内容
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()                                //关闭文件
		kva := mapf(filename, string(content))      //分隔文件中的单词，以kv键值对的形式返回
		intermediate = append(intermediate, kva...) //记录中间结果，由于一般一个MapTask只有一个输入文件,所以一般intermediate和kva中存放的内容相同
	}
	//把intermediate中的数据按key进行hash，并转换成json格式，存储到中间文件
	//中间文件命名格式mr-X-Y	X:MapTaskId		Y:ReducerId 由 hash(key) % ReducerNum确定
	rnum := task.ReducerNum
	for i := 0; i < rnum; i++ {
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _ := os.Create(midFileName)
		enc := json.NewEncoder(midFile)
		for _, kv := range intermediate {
			if ihash(kv.Key)%rnum == i {
				enc.Encode(&kv)
			}
		}
		midFile.Close()
	}
}

// 读取ReduceTask文件中的内容，交给mapf处理
func DoReduceTask(reducef func(string, []string) string, task *Task) {
	//***************如下为shuffle过程***************
	intermediate := []KeyValue{} //记录reducef处理后的中间结果
	//一般一个ReduceTask有多个输入文件,以此读取各中间文件内的数据，并反序列化
	for _, filename := range task.InputFile {
		file, err := os.Open(filename) //打开文件
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(OrderKey(intermediate))
	//***************shuffle完毕***************
	//To ensure that nobody observes partially written files in the presence of crashes,
	//the MapReduce paper mentions the trick of using a temporary file
	//and atomically renaming it once it is completely written.
	//You can use ioutil.TempFile/os.CreateTemp to create a temporary file
	//and os.Rename to atomically rename it.
	dir, _ := os.Getwd()
	tmpfile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		log.Fatal("failed to create temp file", err)
	}
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1 //j记录intermediate[i].Key出现的次数
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values) //输出该key出现的次数

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpfile.Close()
	// reduce的输出文件，命名格式为："mr-out-*",其中*通过Task记录的ReducerId获取
	oname := "mr-out-" + strconv.Itoa(task.ReducerId)
	os.Rename(dir+tmpfile.Name(), dir+oname)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	exitflag := false
	for !exitflag {
		// Lab 1:send an RPC to the coordinator asking for a task.
		taskReply := RequestTask()
		switch taskReply.ReplyFlag {
		case TaskGetted:
			{
				task := taskReply.Task
				if task.TaskType == MapTask {
					DoMapTask(mapf, &task)
					FinishTaskAndReport(task.TaskId)
				} else if task.TaskType == ReduceTask {
					DoReduceTask(reducef, &task)
					FinishTaskAndReport(task.TaskId)
				} else {
					fmt.Println("Unknown task type!!!")
				}
			}
		case WaitPlz:
			{
				time.Sleep(time.Second)
			}
		case FinishAndExit:
			{
				exitflag = true
			}
		default:
			fmt.Println("request task error!!!")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
