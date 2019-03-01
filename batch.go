package main

// Heavily inspired by:
// http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html

import (
    "fmt"
    "sync"
    "time"
    "context"
    "github.com/arslanm/kafka-timescale-adapter/log"
)

// Using WaitGroup to block quit until all workers are done processing
var wg sync.WaitGroup

// Creates a struct that will hold a number of metrics. A work request
// is created by the routine that consumes messages from Kafka. 
type WorkRequest struct {
    Metrics     []string
    NumMetrics  int
}

// The work queue will be used to transfer the metrics from the Kafka
// consumer to the worker routines. 
var WorkQueue = make(chan WorkRequest)

// Once a worker routine receives a work request it will run the 
// handler function in order to process the request. Each worker
// routine runs the handler function within a context to limit 
// handler's time processing the request.
type Handler func(context.Context, int, int, []string, int) (error)

// Creates a new worker 
func NewWorker(id int, handler Handler, timeout time.Duration, retry int, workerQueue chan chan WorkRequest) Worker {
    worker := Worker{
        ID          : id,
        Work        : make(chan WorkRequest),
        Execute     : handler,
        Timeout     : timeout,
        Retry       : retry,
        WorkerQueue : workerQueue,
        QuitChan    : make(chan bool),
    }
    return worker
}

// Worker struct
type Worker struct {
    ID          int
    Work        chan WorkRequest
    Execute     Handler
    Timeout     time.Duration
    Retry       int
    WorkerQueue chan chan WorkRequest
    QuitChan    chan bool
}

// Worker routines call ProcessWork to process a work request.
// ProcessWork first creates a context to set a deadline on the 
// request that will be made to the database, then runs the 
// handler function within a routine. If the function does
// return before the deadline the context will be cancelled.
func (w *Worker) ProcessWork(work WorkRequest, attempt int) bool {
    ctx, cancel := context.WithTimeout(context.Background(), w.Timeout)
    defer cancel()

    done := make(chan bool, 1)
    go func(ctx context.Context, id int, work WorkRequest, ok chan bool) {
        err := w.Execute(ctx, id, attempt, work.Metrics, work.NumMetrics)
        if err != nil {
            ok <- false
        } else {
            ok <- true
        }
    }(ctx, w.ID, work, done)

    select {
        case rc := <-done:
            return rc
        case <-ctx.Done():
            log.Info("msg", "Unable to submit metrics", "timeout", w.Timeout, "error", ctx.Err())
            return false
    }
}

// Each worker routine first adds itself to the queue of available workers
// then waits for a work request or a stop command. 
func (w *Worker) Run() {
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            w.WorkerQueue <- w.Work
            select {
            case work := <-w.Work:
                for attempt := 1 ; attempt <= w.Retry ; attempt++ {
                    rc := w.ProcessWork(work, attempt)
                    if rc == true {
                        break
                    }
                }
            case <-w.QuitChan:
                log.Debug("msg", fmt.Sprintf("Worker #%d is stopping", w.ID))
                return
            }
        }
    }()
}

// Stop a worker routine
func (w *Worker) Stop() {
   go func() {
       w.QuitChan <- true
   }()
}

// Worker queue is a channel that will be used to transfer WorkRequest
// channels in it. Each worker has a WorkRequest channel called Work.
// When a worker becomes available it sends its WorkRequest channel to 
// WorkerQueue, much like giving its address so when there's work the
// worker could be found.
var WorkerQueue chan chan WorkRequest

// Foreman assigns the worker routines work requests as it
// receives them. If all workers are busy with processing requests
// Foreman will block Kafka Consumer from sending more work
// requests until a worker becomes available.
var CanSendMore chan bool

// Foreman will send Quit message to all workers if it receives
// a message to the QuitChan channel. Each worker will then quit
// after they are finished with the requests they're working on
var QuitChan chan bool

// Foreman first creates a WorkerQueue, runs worker routines and 
// waits for a work request. When a request is received it runs
// a routine that assigns the request to the next available worker.
func Foreman(handler Handler, timeout time.Duration, retry int, workerCnt int) {
    WorkerQueue = make(chan chan WorkRequest, workerCnt)
  
    WorkerList := make([]Worker, 0)
    for i := 1; i <= workerCnt; i++ {
        worker := NewWorker(i, handler, timeout, retry, WorkerQueue) 
        worker.Run()
        WorkerList = append(WorkerList, worker)
        log.Info("msg", fmt.Sprintf("Running Worker #%d", i))
    }
  
    CanSendMore = make(chan bool, workerCnt)
    go func() {
        for {
            select {
            case work := <-WorkQueue:
            go func(work WorkRequest) {
                worker := <-WorkerQueue
                CanSendMore <- true
                worker <- work
            }(work)
            }
        }
    }()

    QuitChan = make(chan bool)
    go func() {
        select {
        case <-QuitChan:
            for i := 0 ; i < workerCnt ; i++ {
                WorkerList[i].Stop()
            }
        }
    }()
}
