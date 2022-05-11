package main

import (
	"context"
	crand "crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kvproto "hckvstore/rpc/kvrpc"
	"hckvstore/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	CAUSAL = iota
	STRONG
)

// user who queries a kv store service
type Clerk struct {
	servers     []string
	id          int64
	leaderId    int
	seq         int64
	vectorClock map[string]int32

	consistencyLevel int
}

func MakeId() int64 {
	max := big.NewInt(int64(1) << 62)
	// Reader是一个全局、共享的密码用强随机数生成器
	x, _ := crand.Int(crand.Reader, max)
	res := x.Int64()
	return res
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		servers: servers,
		id:      MakeId(),
		seq:     0,
	}
	return ck
}

func (ck *Clerk) Get(serverId int, key string) string {
	switch ck.consistencyLevel {
	case STRONG:
		args := &kvproto.GetArgs{Key: key}
		id := rand.Intn(len(ck.servers)+10) % len(ck.servers)
		for {
			reply, _ := ck.GetValue(ck.servers[id], args)
			if reply.Value != "" {
				util.DPrintf("server: %v", ck.servers[id])
				return reply.Value
			}
			// 否则换一个server再GET
			id = rand.Intn(len(ck.servers)+10) % len(ck.servers)
		}
	case CAUSAL:
		args := &kvproto.GetInCausalArgs{Key: key, VectorClock: ck.vectorClock}
		id := serverId
		for {
			reply, err := ck.GetValueInCausal(ck.servers[id], args)
			if reply.Success && err == nil {
				return reply.Value
			}
			id = (id + 1) % len(ck.servers)
		}
	default:
		util.DPrintf("Get() consistencyLevel is false")
		return ""
	}
}

func (ck *Clerk) Put(serverId int, key string, value string) bool {
	switch ck.consistencyLevel {
	case STRONG:
		args := &kvproto.PutAppendArgs{Key: key, Value: value, Op: "Put", Id: ck.id, Seq: ck.seq}
		id := ck.leaderId
		for {
			reply, ok := ck.putAppendValue(ck.servers[id], args)
			if ok && reply.IsLeader {
				ck.leaderId = id
				return true
			} else {
				util.DPrintf("can not connect ", ck.servers[id], "or it's not leader")
			}
			id = (id + 1) % len(ck.servers)
		}
	case CAUSAL:
		args := &kvproto.PutAppendInCausalArgs{Key: key, Value: value, Op: "Put", Id: ck.id, Seq: ck.seq, VectorClock: ck.vectorClock}
		id := serverId
		for {
			reply, ok := ck.putAppendValueInCausal(ck.servers[id], args)
			if ok && reply.Success {
				ck.vectorClock = reply.VectorClock
				return true
			} else {
				util.DPrintf("server(%v)'s vectorclock is old, put is failed", ck.servers[id])
			}
			id = (id + 1) % len(ck.servers)
		}
	default:
		util.DPrintf("Put() consistencyLevel is false")
		return false
	}
}

// func (ck *Clerk) Append(key string, value string) bool {
// 	// You will have to modify this function.
// 	args := &kvproto.PutAppendArgs{Key: key, Value: value, Op: "Append", Id: ck.id, Seq: ck.seq}
// 	id := ck.leaderId
// 	for {
// 		reply, ok := ck.putAppendValue(ck.servers[id], args)
// 		if ok && reply.IsLeader {
// 			ck.leaderId = id
// 			return true
// 		}
// 		id = (id + 1) % len(ck.servers)
// 	}
// }

func (ck *Clerk) putAppendValue(address string, args *kvproto.PutAppendArgs) (*kvproto.PutAppendReply, bool) {
	// Initialize Client
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials())) //,grpc.WithBlock())
	if err != nil {
		log.Printf("putAppendValue() did not connect: %v", err)
		return nil, false
	}
	defer conn.Close()
	client := kvproto.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	// 调用Server的putAppend
	var reply *kvproto.PutAppendReply
	reply, err = client.PutAppend(ctx, args)
	if err != nil {
		util.DPrintf("putAppendValue() is failed, error:", err)
		return nil, false
	}
	return reply, true
}

func (ck *Clerk) putAppendValueInCausal(address string, args *kvproto.PutAppendInCausalArgs) (*kvproto.PutAppendInCausalReply, bool) {
	// Initialize Client
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials())) //,grpc.WithBlock())
	if err != nil {
		log.Printf("putAppendValueInCausal() did not connect: %v", err)
		return nil, false
	}
	defer conn.Close()
	client := kvproto.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	// 调用Server的putAppend
	var reply *kvproto.PutAppendInCausalReply
	reply, err = client.PutAppendInCausal(ctx, args)
	if err != nil {
		util.DPrintf("putAppendValueInCausal() is failed, error:", err)
		return nil, false
	}
	return reply, true
}

func (ck *Clerk) GetValue(address string, args *kvproto.GetArgs) (*kvproto.GetReply, error) {
	//  grpc.WithInsecure(): client连接server跳过服务器证书的验证，使用明文通讯，会被第三方监听
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.DPrintf("err: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := kvproto.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.Get(ctx, args)
	if err != nil {
		util.DPrintf("GetValue() is failed")
		return nil, err
	}
	return reply, nil
}

func (ck *Clerk) GetValueInCausal(address string, args *kvproto.GetInCausalArgs) (*kvproto.GetInCausalReply, error) {
	//  grpc.WithInsecure(): client连接server跳过服务器证书的验证，使用明文通讯，会被第三方监听
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.DPrintf("err: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := kvproto.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.GetInCausal(ctx, args)
	if err != nil {
		util.DPrintf("GetValueInCausal() is failed")
		return nil, err
	}
	return reply, nil
}

var count int32 = 0
var putCount int32 = 0
var getCount int32 = 0

func ReadRequest(num int, servers []string) {
	ck := Clerk{
		servers: make([]string, len(servers)),
	}
	copy(ck.servers, servers)
	// for i := 0; i < len(servers); i++ {
	// 	ck.servers[i] = servers[i]
	// }

	// num表示Get的次数
	serverId := 0
	for i := 0; i < num; i++ {
		// 获取Key对应的Value
		value := ck.Get(serverId, "key")
		fmt.Println("value: ", value)
		atomic.AddInt32(&count, 1)
	}

}

func Request(cnum int, num int, servers []string) {
	fmt.Println("servers: ", servers)
	ck := Clerk{
		servers:     make([]string, len(servers)),
		vectorClock: make(map[string]int32),
	}
	ck.consistencyLevel = CAUSAL
	for _, server := range servers {
		ck.vectorClock[server] = 0
	}
	copy(ck.servers, servers)
	start_time := time.Now()
	serverId := 0
	for i := 0; i < num; i++ {
		rand.Seed(time.Now().UnixNano())
		key := rand.Intn(100000)
		value := rand.Intn(100000)
		// 写操作
		ck.Put((serverId+1)%len(ck.servers), "key"+strconv.Itoa(key), "value"+strconv.Itoa(value))
		atomic.AddInt32(&putCount, 1)
		atomic.AddInt32(&count, 1)
		// 读操作
		k := "key" + strconv.Itoa(key)
		v := ck.Get((serverId+1)%len(ck.servers), k)
		atomic.AddInt32(&getCount, 1)
		atomic.AddInt32(&count, 1)
		if v != "" {
			// 查询出了值就输出，屏蔽请求非Leader的情况
			// util.DPrintf("TestCount: ", count, ",Get ", k, ": ", ck.Get(k))
			util.DPrintf("TestCount: %v ,Get %v: %v, VectorClock: %v, getCount: %v, putCount: %v", count, k, v, ck.vectorClock, getCount, putCount)
			util.DPrintf("spent: %v", time.Since(start_time))
		}

		if int(count) == num*cnum {
			util.DPrintf("Task is completed, spent: %v", time.Since(start_time))
		}
		serverId++
	}
}

func RequestRation(cnum int, num int, servers []string, getRation int) {
	fmt.Println("servers: ", servers)
	ck := Clerk{
		servers:     make([]string, len(servers)),
		vectorClock: make(map[string]int32),
	}
	ck.consistencyLevel = CAUSAL
	for _, server := range servers {
		ck.vectorClock[server] = 0
	}
	copy(ck.servers, servers)
	start_time := time.Now()
	serverId := 0
	for i := 0; i < num; i++ {
		rand.Seed(time.Now().UnixNano())
		key := rand.Intn(100000)
		value := rand.Intn(100000)
		// 写操作
		ck.Put((serverId+1)%len(ck.servers), "key"+strconv.Itoa(key), "value"+strconv.Itoa(value))
		atomic.AddInt32(&putCount, 1)
		atomic.AddInt32(&count, 1)

		for j := 0; j < getRation; j++ {
			// 读操作
			k := "key" + strconv.Itoa(key)
			v := ck.Get((serverId+1)%len(ck.servers), k)
			atomic.AddInt32(&getCount, 1)
			atomic.AddInt32(&count, 1)
			if v != "" {
				// 查询出了值就输出，屏蔽请求非Leader的情况
				// util.DPrintf("TestCount: ", count, ",Get ", k, ": ", ck.Get(k))
				util.DPrintf("TestCount: %v ,Get %v: %v, VectorClock: %v, getCount: %v, putCount: %v", count, k, v, ck.vectorClock, getCount, putCount)
				util.DPrintf("spent: %v", time.Since(start_time))
			}
		}

		if int(count) == num*cnum {
			util.DPrintf("Task is completed, spent: %v", time.Since(start_time))
		}
		serverId++
	}
}

func main() {
	var ser = flag.String("servers", "", "the Server, Client Connects to")
	var mode = flag.String("mode", "read", "Read or Put and so on")
	var cnums = flag.String("cnums", "1", "Client Threads Number")
	var onums = flag.String("onums", "1", "Client Requests times")
	var getration = flag.String("getration", "1", "Get Times per Put Times")
	// 将命令行参数解析
	flag.Parse()
	servers := strings.Split(*ser, ",")
	clientNumm, _ := strconv.Atoi(*cnums)
	optionNumm, _ := strconv.Atoi(*onums)
	getRation, _ := strconv.Atoi(*getration)

	if clientNumm == 0 {
		fmt.Println("### Don't forget input -cnum's value ! ###")
		return
	}
	if optionNumm == 0 {
		fmt.Println("### Don't forget input -onumm's value ! ###")
		return
	}

	// 总请求次数Times = clientNumm * optionNumm
	if *mode == "RequestRation" {
		for i := 0; i < clientNumm; i++ {
			go RequestRation(clientNumm, optionNumm, servers, getRation)
		}
	} else if *mode == "Request" {
		for i := 0; i < clientNumm; i++ {
			go Request(clientNumm, optionNumm, servers)
		}
	} else {
		fmt.Println("### Wrong Mode ! ###")
		return
	}
	// fmt.Println("count")
	// time.Sleep(time.Second * 3)
	// fmt.Println(count)
	//	return

	// keep main thread alive
	time.Sleep(time.Second * 1200)
}
