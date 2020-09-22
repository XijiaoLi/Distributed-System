package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}

func DistributeJobs(mr *MapReduce, done_channel chan int, job_type JobType, n_jobs int, n_other_jobs int) {
  for i := 0; i < n_jobs; i++ {
    go func(job_num int) {
      for { // wait for a free worker to finish this current job
        worker := <-mr.registerChannel
        var reply DoJobReply
        args := &DoJobArgs{File: mr.file, Operation: job_type, JobNumber: job_num, NumOtherPhase: n_other_jobs}
        call(worker, "Worker.DoJob", args, &reply)
        done_channel <- job_num
        mr.registerChannel <- worker
        break
      }
    }(i)
  }
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  done_channel := make(chan int)

  //  assign map jobs
  DistributeJobs(mr, done_channel, Map, mr.nMap, mr.nReduce)
  for i := 0; i < mr.nMap; i++ {
    <-done_channel
  }
  // assign reduce jobs
  DistributeJobs(mr, done_channel, Reduce, mr.nReduce, mr.nMap)
  for i := 0; i < mr.nReduce; i++ {
    <-done_channel
  }

  return mr.KillWorkers()
}
