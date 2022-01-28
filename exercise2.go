package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type (
	WorkerError struct {
		Msg    string
		TID    int
		moment time.Time
	}

	WorkerErrorHandler    func(error)
	WorkerStartHandler    func(int)
	WorkerCompleteHandler func(int)
	WorkerHandler         func() error

	Task struct {
		Terminate  bool
		ID         int
		Resolution error
		Worker     WorkerHandler
	}

	Pool struct {
		wg         sync.WaitGroup
		tasks      []*Task
		finished   chan bool
		errChannel chan error
		quit       chan os.Signal
	}
)

func (e *WorkerError) Error() string {

	return e.Msg
}

// Conduct the termination of all the workers
func (p *Pool) shutdown() {

	<-p.quit

	for _, t := range p.tasks {

		t.Terminate = true
	}
}

// Emulates Human being interruption
func doInterruption() {

	process, _ := os.FindProcess(os.Getpid())
	process.Signal(syscall.SIGTERM)
}

// Collects all the error of the tasks
func (p *Pool) collectErrors() []error {

	aerr := make([]error, len(p.tasks))

	for sedIdx, _ := range p.tasks {

		aerr[sedIdx] = p.tasks[sedIdx].Resolution
	}

	return aerr
}

// Prepare pool with the task to execute
func NewPool(tasks ...*Task) *Pool {

	var p *Pool = new(Pool)

	p.tasks = tasks
	p.errChannel = make(chan error, 1)
	p.finished = make(chan bool, 1)

	return p
}

// Execute the pool of workers
// Event handlers let us act on occurance
// of errors, start-off and completion from workers rapidly
// No need to await until everything was done.
func (p *Pool) Run(onErrorHandler WorkerErrorHandler,
	onStart WorkerStartHandler,
	onComplete WorkerCompleteHandler) []error {

	var latestErr error

	p.quit = make(chan os.Signal, 1)
	signal.Notify(p.quit, syscall.SIGINT, syscall.SIGTERM)

	go p.shutdown()

	launcher := func(t *Task) {

		p.wg.Add(1)
		go func() {

			defer func() {
				if !t.Terminate {
					onComplete(t.ID)
				}
				p.wg.Done()
			}()

			onStart(t.ID)

			if t.Resolution = t.Worker(); t.Resolution != nil {

				p.errChannel <- t.Resolution
			}
		}()
	}

	for sedIdx := 0; sedIdx < len(p.tasks); sedIdx++ {

		p.tasks[sedIdx].ID = sedIdx
		launcher(p.tasks[sedIdx])
	}

	go func() {

		p.wg.Wait()
		close(p.finished)
	}()

calderon:

	select {
	case <-p.finished:
		fmt.Printf("Gracefully shuting down\n")
		doInterruption()
	case latestErr = <-p.errChannel:
		onErrorHandler(latestErr)

		/* Go back awaiting for
		   any else error event to catch */
		goto calderon
	}

	close(p.errChannel)

	return p.collectErrors()
}

var onlyOnce sync.Once
var dice = []int{1, 2, 3, 4, 5, 6}

// We did not spend time coding a function to roll a dice
// We simple obtained it from
// https://www.socketloop.com/tutorials/golang-roll-the-dice-example
func rollDice() int {

	onlyOnce.Do(func() {

		rand.Seed(time.Now().UnixNano()) // only run once
	})

	return dice[rand.Intn(len(dice))]
}

const maxAttempts = 4

var interval int

func main() {

	flag.IntVar(&interval, "inter", 5000, "The interval among each roll")
	flag.Parse()

	inceptRoll := func() *Task {

		var t Task

		t.Worker = func() error {

			for attempt := 0; attempt <= maxAttempts; attempt++ {

				if t.Terminate {

					return &WorkerError{
						Msg:    fmt.Sprintf("Worker %d has retired\n", t.ID),
						TID:    t.ID,
						moment: time.Now(),
					}
				}

				// Rolling a dice over and over again
				{
					time.Sleep(time.Duration(interval) * time.Millisecond)
					fmt.Printf("Dice(%d) shows %d\n", t.ID, rollDice())
				}
			}

			return nil
		}

		return &t
	}

	p := NewPool(
		inceptRoll(), // Task 0
		inceptRoll(), // Task 1
		inceptRoll(), // Task 2
		inceptRoll(), // Task 3
		inceptRoll(), // Task 4
		inceptRoll(), // Task 5
		inceptRoll(), // Task 6
		inceptRoll(), // Task 7
	)

	p.Run(func(err error) {

		if err != nil {

			if werr, success := err.(*WorkerError); success {

				fmt.Printf("%s\n", werr.Error())
			}
		}

	}, func(ID int) {

		fmt.Printf("Worker %d has begun\n", ID)

	}, func(ID int) {

		fmt.Printf("Worker %d has done\n", ID)

	})
}
