package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

type Server struct {
	zk *zk.Conn
	id string

	masterId    string
	masterWatch <-chan zk.Event

	// as a master
	tasksWatch   <-chan zk.Event
	tasks        []string
	workersWatch <-chan zk.Event
	workers      []string

	// as a worker
	assignWatch   <-chan zk.Event
	assignedTasks []string // tasks assigned to current server
	runningTasks  map[string]bool

	done chan struct{}
}

func (s *Server) isMaster() bool { return s.id == s.masterId }

func NewServer(zkHost string, id string) *Server {
	if id == "" {
		panic("empty id")
	}
	c, _, err := zk.Connect([]string{zkHost}, time.Second, zk.WithLogInfo(false))
	if err != nil {
		panic("cannot connect to zk server")
	}
	return &Server{
		zk:           c,
		id:           id,
		runningTasks: map[string]bool{},
		done:         make(chan struct{}, 1),
	}
}

// run a event loop
func (s *Server) run() error {
	if err := s.bootstrap(); err != nil {
		return err
	}

	s.watchMaster()
	for {
		// as a worker
		if len(s.assignedTasks) > 0 {
			for _, task := range s.assignedTasks {
				if s.runningTasks[task] {
					continue
				}
				s.runningTasks[task] = true
				go s.runTask(task)
			}
		}

		// as a master
		if len(s.tasks) > 0 && len(s.workers) > 0 {
			n := len(s.workers)
			for _, task := range s.tasks {
				cmd, err := s.getTask(task)
				if err != nil {
					return err
				}
				worker := s.workers[rand.Intn(n)]
				if err := s.assignTask(task, cmd, worker); err != nil {
					return err
				}
				log.Printf("[%s] assigned to %s\n", task, worker)
			}
		}

		select {
		case <-s.masterWatch:
			s.watchMaster()
		case <-s.workersWatch:
			s.watchWorkers()
		case <-s.tasksWatch:
			s.watchTasks()
		case <-s.assignWatch:
			s.watchAssign()
		case <-s.done:
			log.Println("server shutdown")
			s.close()
			return nil
		}
	}
}

func (s *Server) stop() {
	select {
	case s.done <- struct{}{}:
	default:
	}
}

func (s *Server) close() {
	s.zk.Close()
}

func (s *Server) bootstrap() error {
	var err error
	createParent := func(path string) {
		if err != nil {
			return
		}
		for {
			_, err = s.zk.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
			switch err {
			case zk.ErrNodeExists:
				err = nil
				return
			case zk.ErrConnectionClosed:
				time.Sleep(time.Millisecond * 20)
			default:
				return
			}
		}
	}
	createParent("/workers")
	createParent("/assign")
	createParent("/tasks")
	createParent("/status")
	return err
}

// set the master id and do the setup as a master or slave if necessary
func (s *Server) setMaster(id string) {
	// avoid duplicated setup on role switching
	if s.masterId == id {
		return
	}

	log.Printf("Master:%s\n", id)
	s.masterId = id
	if s.isMaster() {
		// reset workers
		s.assignWatch = nil
		s.assignedTasks = nil

		s.watchWorkers()
		s.watchTasks()
	} else {
		// reset master
		s.tasksWatch = nil
		s.tasks = nil
		s.workersWatch = nil
		s.workers = nil

		s.registerWorker()
		s.createAssign()
		s.watchAssign()
	}
}

func (s *Server) electMaster() {
	_, err := s.zk.Create("/master", []byte(s.id), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	switch err {
	case nil:
		s.setMaster(s.id)
		log.Println("elect success")
		s.watchMaster()
	case zk.ErrNodeExists:
		// someone else is the master
		s.watchMaster()
	case zk.ErrConnectionClosed:
		log.Println("connection lost, keep watching master")
		s.watchMaster()
	default:
		log.Printf("failed to elect master, err = %v\n", err)
		s.stop()
	}
}

func (s *Server) watchMaster() {
	data, _, ch, err := s.zk.GetW("/master")
	switch err {
	case nil:
		id := string(data)
		s.setMaster(id)
		s.masterWatch = ch
	case zk.ErrNoNode:
		log.Println("no master, electing...")
		s.electMaster()
	case zk.ErrConnectionClosed:
		log.Println("connection lost, retry set master watch")
		s.watchMaster()
	default:
		log.Printf("failed to set master watch, err= %v\n", err)
		s.stop()
	}
}

// register as a worker
func (s *Server) registerWorker() {
	// create a ephemeral node to keep alive
	_, err := s.zk.Create("/workers/"+s.id, []byte("Idle"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	switch err {
	case nil, zk.ErrNodeExists:
		// fixme should reset the worker state as IDLE synchronously if node exists
		log.Println("register as a worker")
	case zk.ErrConnectionClosed:
		log.Println("connection lost, retry register as a worker")
		s.registerWorker()
	default:
		log.Printf("failed to register as a worker, err= %v\n", err)
		s.stop()
	}
}

// create a persistent parent node to store tasks
func (s *Server) createAssign() {
	parentNode := "/assign/" + s.id
	_, err := s.zk.Create(parentNode, []byte{}, 0, zk.WorldACL(zk.PermAll))
	switch err {
	case nil, zk.ErrNodeExists:
		break
	case zk.ErrConnectionClosed:
		log.Println("connection lost, retry register as a worker")
		s.registerWorker()
	default:
		log.Printf("failed to create parent node %s, err= %v\n", parentNode, err)
		s.stop()
	}
}

func (s *Server) watchAssign() {
	path := "/assign/" + s.id
	tasks, _, ch, err := s.zk.ChildrenW(path)
	switch err {
	case nil:
		log.Println("Tasks:")
		for _, task := range tasks {
			log.Printf("\t%s\n", task)
		}
		s.assignedTasks = tasks
		s.assignWatch = ch
	case zk.ErrNoNode:
		log.Printf("[panic] path %s not exists\n", path)
		s.stop()
	case zk.ErrConnectionClosed:
		log.Println("connection lost, retry set assign watch")
		s.watchAssign()
	default:
		log.Printf("failed to set assign watch, err= %v\n", err)
		s.stop()
	}
}

func (s *Server) watchWorkers() {
	workers, _, ch, err := s.zk.ChildrenW("/workers")
	switch err {
	case nil:
		if err := s.updateWorkersView(workers); err != nil {
			log.Printf("migrate absent tasks error")
			s.stop()
			return
		}
		s.workersWatch = ch
	case zk.ErrNoNode:
		log.Println("[panic] path /workers not exists")
		s.stop()
	case zk.ErrConnectionClosed:
		log.Println("connection lost, retry set workers watch")
		s.watchWorkers()
	default:
		log.Printf("failed to set workers watch, err= %v\n", err)
		s.stop()
	}
}

func (s *Server) updateWorkersView(workers []string) error {
	// count the workers left
	//
	// todo
	// calculate the diff between local and zk workers views is not necessary.
	// you can just compare the /assign/* and the /workers/* directly.
	// the benefit of this approach is that it can handle crash of the master on reassigning also.
	var leftWorkers []string
	for _, old := range s.workers {
		found := false
		for _, new := range workers {
			if old == new {
				found = true
				break
			}
		}
		if !found {
			leftWorkers = append(leftWorkers)
		}
	}
	s.workers = workers
	log.Println("Workers:")
	for _, worker := range workers {
		log.Printf("\t%s\n", worker)
	}

	// move absent tasks back to the /tasks pool
	for _, worker := range leftWorkers {
		absentTasks, err := s.getWorkerTasks(worker)
		if err != nil {
			return fmt.Errorf("failed to get tasks of worker %v", worker)
		}
		for _, task := range absentTasks {
			cmd, err := s.getAssignedTask(worker, task)
			if err != nil {
				return err
			} else if cmd == "" {
				continue
			}
			if err := s.createTask(task, cmd); err != nil {
				return err
			}
			if err := s.removeAssignedTask(worker, task); err != nil {
				return err
			}
			log.Printf("worker %s task %s reassined\n", workers, task)
		}
	}
	return nil
}

func (s *Server) getWorkerTasks(worker string) ([]string, error) {
	absentTasks, _, err := s.zk.Children("/assign/" + worker)
	switch err {
	case nil:
		return absentTasks, nil
	case zk.ErrNoNode:
		return nil, nil
	case zk.ErrConnectionClosed:
		return s.getWorkerTasks(worker)
	default:
		return nil, err
	}
}

func (s *Server) getAssignedTask(worker string, task string) (string, error) {
	path := "/assign/" + worker + "/" + task
	data, _, err := s.zk.Get(path)
	switch err {
	case nil:
		return string(data), nil
	case zk.ErrNoNode:
		log.Printf("worker %s task %s not found\n", worker, task)
		return "", nil
	case zk.ErrConnectionClosed:
		return s.getAssignedTask(worker, task)
	default:
		return "", err
	}
}

func (s *Server) removeAssignedTask(worker string, task string) error {
	path := "/assign/" + worker + "/" + task
	err := s.zk.Delete(path, -1)
	switch err {
	case nil:
		return nil
	case zk.ErrNoNode:
		log.Printf("worker %s task %s not found\n", worker, task)
		return nil
	case zk.ErrConnectionClosed:
		return s.removeAssignedTask(worker, task)
	default:
		return err
	}
}

func (s *Server) createTask(name string, cmd string) error {
	_, err := s.zk.Create("/tasks/"+name, []byte(cmd), 0, zk.WorldACL(zk.PermAll))
	switch err {
	case nil:
		return nil
	case zk.ErrNodeExists:
		return nil
	case zk.ErrConnectionClosed:
		return s.createTask(name, cmd)
	default:
		return err
	}
}

// todo
// worker go offline will lead to the migrating of absent worker tasks in the master.
// with the numbers of left workers and their tasks goes big, the notify of tasks watch could be very frequent.
// it may alleviate the problem to pause the tasks watch when migrating absent tasks.
func (s *Server) watchTasks() {
	tasks, _, ch, err := s.zk.ChildrenW("/tasks")
	switch err {
	case nil:
		s.tasks = tasks
		s.tasksWatch = ch

		log.Println("Tasks:")
		for _, task := range tasks {
			log.Printf("\t%s\n", task)
		}
	case zk.ErrNoNode:
		log.Println("[panic] path /tasks not exists")
		s.stop()
	case zk.ErrConnectionClosed:
		log.Println("connection lost, retry set tasks watch")
		s.watchTasks()
	default:
		log.Printf("failed to set tasks watch, err= %v\n", err)
		s.stop()
	}
}

func (s *Server) getTask(task string) (string, error) {
	data, _, err := s.zk.Get("/tasks/" + task)
	switch err {
	case nil:
		return string(data), nil
	case zk.ErrNoNode:
		return "", nil
	case zk.ErrConnectionClosed:
		return s.getTask(task)
	default:
		log.Printf("failed to get task, err= %v", err)
		return "", err
	}
}

// todo atomically move task from /tasks to /assign/worker
func (s *Server) assignTask(task string, cmd string, worker string) error {
	_, err := s.zk.Create("/assign/"+worker+"/"+task, []byte(cmd), 0, zk.WorldACL(zk.PermAll))
	switch err {
	case nil, zk.ErrNodeExists:
		return s.removeTask(task)
	case zk.ErrConnectionClosed:
		return s.assignTask(task, cmd, worker)
	default:
		return err
	}
}

func (s *Server) removeTask(task string) error {
	err := s.zk.Delete("/tasks/"+task, -1)
	switch err {
	case zk.ErrNoNode:
		return nil
	case zk.ErrConnectionClosed:
		return s.removeTask(task)
	default:
		return err
	}
}

func (s *Server) runTask(task string) {
	cmd, err := s.getAssignedTask(s.id, task)
	if err != nil {
		log.Printf("failed to get task, err= %v\n", err)
		return
	}

	// simulate task running
	startAt := time.Now()
	log.Printf("[%s] exec %s\n", task, cmd)
	time.Sleep(time.Duration(1+rand.Intn(10)) * time.Second)
	log.Printf("[%s] done in %v", task, time.Since(startAt))

	if err := s.setTaskStatus(task, "success"); err != nil {
		log.Printf("task %s done, but update status failed, err= %v", task, err)
		return
	}
	if err := s.removeAssignedTask(s.id, task); err != nil {
		log.Printf("task %s status updated, but remove task failed, err= %v", task, err)
		return
	}
	log.Printf("task %s done", task)
}

func (s *Server) setTaskStatus(task string, status string) error {
	_, err := s.zk.Create("/status/"+task, []byte(status), 0, zk.WorldACL(zk.PermAll))
	switch err {
	case zk.ErrNodeExists:
		return nil
	case zk.ErrConnectionClosed:
		return s.setTaskStatus(task, status)
	default:
		return err
	}
}

func (s *Server) watchTaskStatus(task string) (bool, <-chan zk.Event, error) {
	exists, _, ch, err := s.zk.ExistsW("/status/" + task)
	switch err {
	case nil:
		return exists, ch, nil
	case zk.ErrConnectionClosed:
		return s.watchTaskStatus(task)
	default:
		return false, nil, err
	}
}

func (s *Server) getTaskStatus(task string) (string, error) {
	data, _, err := s.zk.Get("/status/" + task)
	switch err {
	case nil:
		return string(data), nil
	case zk.ErrNoNode:
		return "", fmt.Errorf("status not found")
	case zk.ErrConnectionClosed:
		return s.getTaskStatus(task)
	default:
		return "", nil
	}
}

func (s *Server) submitTask(cmd string) (string, error) {
	path, err := s.zk.Create("/tasks/task-", []byte(cmd), zk.FlagSequence, zk.WorldACL(zk.PermAll))
	switch err {
	case nil:
		return strings.TrimPrefix(path, "/tasks/"), nil
	case zk.ErrNodeExists:
		return "", fmt.Errorf("tasks exists")
	case zk.ErrConnectionClosed:
		// we cannot make sure the node is created
		return s.submitTask(cmd)
	default:
		return "", err
	}
}
