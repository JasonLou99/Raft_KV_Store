package rpctest

import (
	"encoding/json"
	"hckvstore/config"
	"log"
	"testing"
)

func TestJson(t *testing.T) {
	op := &config.Op{
		Option: "Put",
		Key:    "1",
	}
	op2 := &config.Op{}
	data, _ := json.Marshal(op)
	log.Println(op)
	json.Unmarshal(data, op2)
	log.Println(op2)
}
