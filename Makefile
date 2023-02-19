run-node:
	go run . --name $(name)

submit-task:
	go run . --name client --cmd $(cmd)
