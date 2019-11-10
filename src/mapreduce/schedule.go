package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup
	var quit chan int
	quit = make(chan int)

	var taskFunc func(i int, freeWorkerAddress string)
	taskFunc = func(i int, freeWorkerAddress string) {
		var args DoTaskArgs
		var file string
		if phase == mapPhase {
			file = mapFiles[i]
			args = DoTaskArgs{jobName, file, phase, i, n_other}
		}
		args = DoTaskArgs{jobName, file, phase, i, n_other}
		ret := call(freeWorkerAddress, "Worker.DoTask", args, nil)
		fmt.Printf("call %d, ret = %v\n", i, ret)
		if !ret {
			log.Printf("Worker %s might be broken.\n", freeWorkerAddress)
			freeWorkerAddress = <-registerChan
			go taskFunc(i, freeWorkerAddress)
		} else {
			fmt.Printf("release %s at %d, channel_size = %d, ntasks = %d\n", freeWorkerAddress, i, len(registerChan), ntasks)
			fmt.Println("caps = ", cap(registerChan))
			wg.Done()
			select {
			case <-quit:
			case registerChan <- freeWorkerAddress:
			}
		}
	}

	for i := 0; i < ntasks; i++ {
		var freeWorkerAddress string
		freeWorkerAddress = <-registerChan
		wg.Add(1)
		go taskFunc(i, freeWorkerAddress)
	}
	wg.Wait()
	close(quit)

	fmt.Printf("Schedule: %v done\n", phase)
}
