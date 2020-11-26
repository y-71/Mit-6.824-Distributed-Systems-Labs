package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mutex = &sync.Mutex{}

type Master struct {
	// Your definitions here.
	files         []string 	// file pool 
	mapStatus     []int		// status of the map phase 
	reduceStatus  []int		// status of the reduce phase
	numReducers   int		// the number of reducers
	mapCounter    int		// the count of current mappers
	reduceCounter int		// the count of current reduces 
}

// Your code here -- RPC handlers for the worker to call.

//
// Here the implementation of my RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (m *Master) GetTask(request *Request, reply *Task) error {
	
	mutex.Lock()
	defer mutex.Unlock()
	//
	//	Updating the master after the worker finishes it's assigned task
	//
	if request.HasStatus {
		switch request.OfType {
		case "map":
			m.mapStatus[request.Index] = Finished
			m.mapCounter++
		case "reduce":
			m.reduceStatus[request.Index] = Finished
			m.reduceCounter++
		default:
			log.Fatal("Invalid response type")

		}
	}
	//
	//	Assign the tasks to workers 
	//
	if m.mapCounter != len(m.files) {

		for i := range m.files {
			if m.mapStatus[i] == Unallocated {
				reply.OfType = "map"
				reply.Filename = m.files[i]
				reply.Index = i
				reply.NumReducers = m.numReducers
				m.mapStatus[i] = Allocated

				go func(i int) {
					//	the master wait for ten seconds; 
					//	after that the master should assume the worker has died
					time.Sleep(10 * time.Second)

					mutex.Lock()
					defer mutex.Unlock()
					if m.mapStatus[i] == Allocated {
						m.mapStatus[i] = Unallocated
					}
				}(i)
				break
			}

		}
		if reply.OfType == "" {

			reply.Sleep = true
		}
	} else if m.reduceCounter != m.numReducers {

		for i := 0; i < m.numReducers; i++ {
			if m.reduceStatus[i] == Unallocated {
				reply.OfType = "reduce"
				reply.Index = i
				reply.NumOfFiles = len(m.files)
				m.reduceStatus[i] = Allocated
				go func(i int) {
					//	the master wait for ten seconds; 
					//	after that the master should assume the worker has died
					time.Sleep(10 * time.Second)

					mutex.Lock()
					defer mutex.Unlock()
					if m.reduceStatus[i] == Allocated {
						m.reduceStatus[i] = Finished
					}
				}(i)
				break
			}

		}
		if reply.OfType == "" {
			reply.Sleep = true
		}

	} else {
		reply.Completed = true

	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	if m.mapCounter == len(m.files) && m.reduceCounter == m.numReducers {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	
	// Your code here.
	m := Master{files, make([]int, len(files)), make([]int, nReduce), nReduce, 0, 0}
	
	m.server()

	return &m
}
