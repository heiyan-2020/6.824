package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mu                    sync.Mutex
	allDone               bool       // 记录任务是否全部完成，用于快速响应 Done()
	mapTasks              []int      // map task number -> state
	reduceTasks           []int      // reduce task number -> state
	inputFiles            []string   // map task number -> corresponding input file
	intermediateFiles     [][]string // input_file -> file list
	mapTaskElapsedTime    []time.Time
	reduceTaskElapsedTime []time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Ask(args *ExampleArgs, reply *AskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// iterate all tasks in order to find an un-completed task.
	mapAllDone := true
	for index, state := range c.mapTasks {
		if state == 0 {
			reply.TaskType = 0
			reply.TaskIndex = index
			reply.ReduceNum = len(c.reduceTasks)
			reply.Files = append(reply.Files, c.inputFiles[index])
			c.mapTasks[index] = 1
			c.mapTaskElapsedTime[index] = time.Now()
			return nil
		}
		if state == 1 {
			mapAllDone = false
			if time.Now().Sub(c.mapTaskElapsedTime[index]) >= 10*time.Second {
				fmt.Println("Backup")
				c.mapTasks[index] = 0
				c.mapTaskElapsedTime[index] = time.Now()
				reply.TaskType = 0
				reply.TaskIndex = index
				reply.ReduceNum = len(c.reduceTasks)
				reply.Files = append(reply.Files, c.inputFiles[index])
				return nil
			}
		}
	}

	if mapAllDone {
		reduceAllDone := true
		for index, state := range c.reduceTasks {
			if state == 0 {
				for _, files := range c.intermediateFiles {
					// invariant: intermediate files should be sorted.
					reply.Files = append(reply.Files, files[index])
				}
				reply.TaskType = 1
				reply.TaskIndex = index
				reply.ReduceNum = index
				c.reduceTasks[index] = 1
				c.reduceTaskElapsedTime[index] = time.Now()
				return nil
			}
			if state == 1 {
				reduceAllDone = false
				if time.Now().Sub(c.reduceTaskElapsedTime[index]) >= 10*time.Second {
					fmt.Println("Backup")
					c.reduceTasks[index] = 0
					c.reduceTaskElapsedTime[index] = time.Now()
					reply.TaskType = 1
					reply.TaskIndex = index
					reply.ReduceNum = index
					for _, files := range c.intermediateFiles {
						// invariant: intermediate files should be sorted.
						reply.Files = append(reply.Files, files[index])
					}
					return nil
				}
			}
		}
		c.allDone = mapAllDone && reduceAllDone
	}
	// Can't find feasible task, return empty reply.
	return nil
}

func (c *Coordinator) CommitMapTask(args *MapCommitArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTasks[args.TaskIndex] != 2 {
		c.mapTasks[args.TaskIndex] = 2 // change state to completed
		c.intermediateFiles[args.TaskIndex] = args.IntermediateFiles
	}
	return nil
}

func (c *Coordinator) CommitReduceTask(args *ReduceCommitArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reduceTasks[args.TaskIndex] = 2 // change state to completed
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.allDone
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapTasks = make([]int, len(files))
	c.reduceTasks = make([]int, nReduce)
	c.intermediateFiles = make([][]string, len(files))
	c.inputFiles = make([]string, len(files))
	c.mapTaskElapsedTime = make([]time.Time, len(files))
	c.reduceTaskElapsedTime = make([]time.Time, nReduce)
	for i, file := range files {
		c.intermediateFiles[i] = make([]string, nReduce)
		c.inputFiles[i] = file
	}

	c.server()
	return &c
}
