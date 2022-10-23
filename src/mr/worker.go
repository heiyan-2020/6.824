package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		reply := AskReply{}
		ret := CallAsk(&reply)
		if !ret {
			return
		}
		//randNum := rand.Intn(10)
		//if randNum < 5 {
		//	time.Sleep(5 * time.Second)
		//}
		if reply.Files == nil {
			// empty task, which means there isn't task now, sleep for a while.
			time.Sleep(1 * time.Second)
		} else {
			if reply.TaskType == 0 {
				intermediate := mapToIntermediate(reply.Files[0], mapf)
				filenames := partitionAndWrite(intermediate, reply.ReduceNum, reply.Files[0], reply.TaskIndex)
				sort.Strings(filenames)
				args := MapCommitArgs{
					TaskIndex:         reply.TaskIndex,
					IntermediateFiles: filenames,
				}
				CallMapCommit(&args)
			} else {
				populate(reply.TaskIndex, reply.Files, reducef)
				args := ReduceCommitArgs{
					TaskIndex: reply.TaskIndex,
				}
				CallReduceCommit(&args)
			}
		}

	}

}

func mapToIntermediate(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

func partitionAndWrite(intermediate []KeyValue, nReduce int, filename string, taskIndex int) []string {
	files := make(map[int]*os.File)
	filenames := make([]string, 0)
	for i := 0; i < nReduce; i += 1 {
		name := fmt.Sprintf("mr-%d-%d", taskIndex, i)
		filenames = append(filenames, name)
		file, err := os.Create(name)
		if err != nil {
			log.Fatalf("cannot create %v", name)
		}
		files[i] = file
	}
	for _, pair := range intermediate {
		hashedIndex := ihash(pair.Key) % nReduce
		involvedFile := files[hashedIndex]
		fmt.Fprintf(involvedFile, "%v %v\n", pair.Key, pair.Value)
	}

	for _, file := range files {
		file.Close()
	}

	return filenames
}

func populate(taskIndex int, filenames []string, reducef func(string, []string) string) {
	var contents string
	for _, name := range filenames {
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open %v", name)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", name)
		}
		file.Close()
		contents += string(content)
	}

	rawPairs := strings.Split(contents, "\n")
	allPairs := make([]KeyValue, 0)
	for _, rawPair := range rawPairs {
		if rawPair == "" {
			continue
		}
		convertedPair := strings.Split(rawPair, " ")
		allPairs = append(allPairs, KeyValue{convertedPair[0], convertedPair[1]})
	}

	sort.Sort(ByKey(allPairs))

	tmpFile, _ := ioutil.TempFile("", "")
	for i := 0; i < len(allPairs); {
		j := i + 1
		for j < len(allPairs) && allPairs[j].Key == allPairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allPairs[k].Value)
		}
		output := reducef(allPairs[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", allPairs[i].Key, output)

		i = j
	}

	outPath := fmt.Sprintf("mr-out-%d", taskIndex)

	os.Rename(tmpFile.Name(), outPath)

	tmpFile.Close()
}

func CallAsk(reply *AskReply) bool {
	args := ExampleArgs{}
	return call("Coordinator.Ask", &args, reply)
}

func CallMapCommit(args *MapCommitArgs) bool {
	reply := ExampleReply{}
	return call("Coordinator.CommitMapTask", args, &reply)
}

func CallReduceCommit(args *ReduceCommitArgs) bool {
	reply := ExampleReply{}
	return call("Coordinator.CommitReduceTask", args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
