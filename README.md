# A simple implementation of Master-Worker Pattern with ZooKeeper in Golang

> An example code for
[《Apache ZooKeeper - O'Reilly》](https://www.oreilly.com/library/view/zookeeper/9781449361297/)
written in Golang.

## Usage

Run some server nodes

```shell
$ make run-node name=s1
$ make run-node name=s2
$ make run-node name=s3
```

Submit a task

```shell
$ make submit-task cmd=hello
```
