package gossip

import (
	"fmt"
	"hckvstore/config"
)

type Log struct {
	Command config.Op
}

type Gossip struct {
	logs []Log
	// mu      *sync.Mutex
	address string
	// peers   []string
	// applyCh chan int
}

func (gossip *Gossip) Start(command config.Op) {
	log := Log{
		Command: command,
	}
	gossip.logs = append(gossip.logs, log)
	fmt.Println(log)
	// 传播日志
}

func (gossip *Gossip) RegisterServer(address string) {

}

// 让Gossip开始后台运行的进程
// func (gossip *Gossip) Run() {
// 	go
// }

// 初始化一个Gossip实例
// func MakeGossip(address string, peers []string,
// 	mu *sync.Mutex, applyCh chan int) *Gossip {
// 	if len(peers) < 1 {
// 		panic("Need to Set Peers, at Least 1")
// 	}
// 	gossip := &Gossip{
// 		address: address,
// 		mu:      mu,
// 		peers:   peers,
// 		applyCh: applyCh,
// 	}
// 	gossip.logs = append(gossip.logs, Log{
// 		Command: config.Op{
// 			Option: config.Initial,
// 			Key:    "",
// 			Value:  "",
// 		},
// 	})
// 	// gossip.Run()
// 	return gossip
// }
func MakeGossip(address string) *Gossip {
	gossip := &Gossip{
		address: address,
	}
	gossip.logs = append(gossip.logs, Log{
		Command: config.Op{
			Option: "Initial",
			Key:    "",
			Value:  "",
		},
	})
	// gossip.Run()
	return gossip
}
