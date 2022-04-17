package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	kvproto "hckvstore/rpc/kvrpc"

	gsp "hckvstore/gossip"
	raft "hckvstore/raft"

	config "hckvstore/config"
	pst "hckvstore/persister"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type KVServer struct {
	gossip    *gsp.Gossip
	raft      *raft.Raft
	persister *pst.Persister
	applyCh   chan int
	delay     int
}

func (kv *KVServer) Get(ctx context.Context, args *kvproto.GetArgs) (*kvproto.GetReply, error) {
	getReply := &kvproto.GetReply{}
	_, isLeader := kv.raft.GetState()
	getReply.IsLeader = isLeader
	if !isLeader {
		// value is ""
		return getReply, nil
	}
	// 生成操作对应的日志
	op := config.Op{
		Option: "Get",
		Key:    args.Key,
	}
	// 向Raft集群写入日志
	_, _, isLeader = kv.raft.Start(op)
	if !isLeader {
		// value is ""
		return getReply, nil
	}
	getReply.IsLeader = true
	// Get直接让Leader返回结果
	getReply.Value = string(kv.persister.Get(args.Key))
	return getReply, nil
}

func (kv *KVServer) PutAppend(ctx context.Context, args *kvproto.PutAppendArgs) (*kvproto.PutAppendReply, error) {
	putAppendReply := &kvproto.PutAppendReply{}
	_, isLeader := kv.raft.GetState()
	putAppendReply.IsLeader = isLeader
	if !isLeader {
		return putAppendReply, nil
	}
	op := config.Op{
		Option: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Id:     args.Id,
		Seq:    args.Seq,
	}
	index, _, isLeader := kv.raft.Start(op)
	if !isLeader {
		// 说明这个过程中 membership发生了变化
		fmt.Println("Leader Changed !")
		putAppendReply.IsLeader = false
		return putAppendReply, nil
	}

	// 如果kv.applyCh空，这里就会阻塞
	apply := <-kv.applyCh
	fmt.Println("PutAppend apply success, index: ", index)
	if apply == 1 {
	}
	return putAppendReply, nil
}

func (kv *KVServer) RegisterServer(address string) {
	// Register Server
	for {
		lis, err := net.Listen("tcp", address)
		fmt.Println(address)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		// 构造grpc服务对象
		grpcServer := grpc.NewServer()
		// kv需要实现proto内的所有service才可以注册
		// 注册service到kv中
		kvproto.RegisterKVServer(grpcServer, kv)
		// Register reflection service on gRPC server.
		reflection.Register(grpcServer)
		// 在一个监听端口提供grpc服务
		if err := grpcServer.Serve(lis); err != nil {
			fmt.Println("failed to serve: ", err)
		}
	}

}

func main() {
	var add = flag.String("address", "", "Input Your address")
	var mems = flag.String("members", "", "Input Your follower")
	// var delays = flag.String("delay", "", "Input Your follower")
	flag.Parse()
	address := *add
	members := strings.Split(*mems, ",")
	// delay, _ := strconv.Atoi(*delays)

	kvserver := &KVServer{}
	persister := &pst.Persister{}
	kvserver.persister = persister
	// kvserver.persister.Init("/home/jason/hybrid_consistency/db/" + address)
	kvserver.persister.Init("../db/" + address)
	// 缓冲通道可以并行处理100个log apply
	kvserver.applyCh = make(chan int, 100)
	go kvserver.RegisterServer(address + "1")
	// delay 默认为0，同一数据中心内
	kvserver.delay = 0
	kvserver.gossip = gsp.MakeGossip(address)
	kvserver.raft = raft.MakeRaft(address, members, persister, &sync.Mutex{}, kvserver.applyCh, kvserver.delay)

	// server运行20min
	time.Sleep(time.Second * 1200)
}
