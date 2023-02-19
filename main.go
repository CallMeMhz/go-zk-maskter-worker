package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	name := flag.String("name", "", "")
	cmd := flag.String("cmd", "", "")
	flag.Parse()

	if name == nil || *name == "" {
		log.Fatal("name required")
	}
	s := NewServer("127.0.0.1", *name)
	if cmd != nil && *cmd != "" {
		submitTask(s, *cmd)
		s.stop()
		return
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exit
		s.stop()
	}()

	if err := s.run(); err != nil {
		log.Fatalf("failed to start server, err = %v", err)
	}
}

func submitTask(s *Server, cmd string) {
	time.Sleep(time.Second)
	task, err := s.submitTask(cmd)
	if err != nil {
		log.Printf("submit task failed, err= %v\n", err)
		return
	}
	log.Printf("submit task succeess, id= %v\n", task)

	exists, watch, err := s.watchTaskStatus(task)
	if err != nil {
		log.Printf("cannot set task status watch, err= %v\n", err)
		return
	}
	if !exists {
		log.Println("wait ...")
		<-watch
	}
	status, err := s.getTaskStatus(task)
	if err != nil {
		log.Printf("cannot get task status, err= %v\n", err)
		return
	}
	log.Printf("task status update: %v\n", status)
}
