package leveldbtest

import (
	pst "hckvstore/persister"
	"log"
	"testing"
)

// t参数用于报告测试失败和附加的日志信息
func TestAPI(t *testing.T) {
	persister := &pst.Persister{}
	persister.Init("./block")
	persister.Put("key_1", "value_1")
	value := string(persister.Get("key_1"))
	log.Println(value)
}
