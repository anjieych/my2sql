package base

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	SQL "my2sql/sqlbuilder"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/segmentio/kafka-go"
)

const Op4StarRocks = "__op"

//格式化binlog 为Maxwell json,并输出到kafka
//kafka topic为 {prefix}_{database}_{table}

func getOp4StarRocksByBinlogSqlType(sqlType string) int {
	switch sqlType {
	case "insert", "update":
		return 0
	case "delete":
		return 1
	default:
		return -1
	}
}

// Maxwell value(binloga0
type Maxwellvalue struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     string                 `json:"type"`
	Ts       uint32                 `json:"ts"`
	Xid      uint64                 `json:"xid"`
	Position string                 `json:"position"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old"`
	Commit   bool                   `json:"commit"`
	Xoffset  int64                  `json:"xoffset"`
}

type Maxwell struct {
	Topic string                 `json:"topic"` //格式：kafka topic为 {prefix}_{database}_{table}
	Key   map[string]interface{} `json:"key"`   //格式：{ "database":"test_tb","table":"test_tbl","pk.id":4,"pk.part2":"hello"}
	Value *Maxwellvalue          `json:"value"`
}

func GenMaxwellForOneRowsEvent(sqlType string, db string, tb string, posStr string, timestamp uint32, eventIdx uint64, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, rowsPerSql int,
	ifRollback bool, ifprefixDb bool, ifIgnorePrimary bool, primaryIdx []int, topicPrifix string) kafka.Message {

	maxwell := Maxwell{
		Topic: fmt.Sprintf("%s_%s_%s", topicPrifix, db, tb),
		Key:   genMaxwellKey(sqlType, db, tb, rEv, colDefs, primaryIdx),
		Value: genMaxwellValue(sqlType, db, tb, posStr, timestamp, eventIdx, rEv, colDefs),
	}
	key, _ := json.Marshal(maxwell.Key)
	value, _ := json.Marshal(maxwell.Value)
	kmessage := kafka.Message{
		Topic: maxwell.Topic,
		Key:   key,
		Value: value,
	}
	return kmessage
}

// 生成 key
func genMaxwellKey(sqlType string, db string, tb string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, primaryIdx []int) map[string]interface{} {
	mkey := make(map[string]interface{})
	mkey["database"] = db
	mkey["table"] = tb

	var rowAfter []interface{}
	if sqlType == "update" {
		rowAfter = rEv.Rows[1]
	} else {
		rowAfter = rEv.Rows[0]
	}

	for _, v := range primaryIdx {
		mkey["pk."+colDefs[v].Name()] = rowAfter[v]
	}
	return mkey
}

// 生成 value["data"]
func _genMaxwellValueOfData(sqlType string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn) map[string]interface{} {
	data := make(map[string]interface{})
	data[Op4StarRocks] = getOp4StarRocksByBinlogSqlType(sqlType)
	var rowAfter []interface{}

	if sqlType == "update" {
		rowAfter = rEv.Rows[1]
	} else {
		rowAfter = rEv.Rows[0]
	}

	for i, v := range rowAfter {
		data[colDefs[i].Name()] = v
	}
	return data
}

// 生成 value["old"]
func _genMaxwellValueOfOld(sqlType string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn) map[string]interface{} {
	old := make(map[string]interface{})
	old[Op4StarRocks] = 1

	var rowBefore []interface{}
	if sqlType == "update" {
		rowBefore = rEv.Rows[0]

	}

	for i, v := range rowBefore {
		old[colDefs[i].Name()] = v
	}
	return old
}

// 生成 value
func genMaxwellValue(sqlType string, db string, tb string, posStr string, timestamp uint32, eventIdx uint64, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn) *Maxwellvalue {
	mvalue := &Maxwellvalue{}
	mvalue.Database = db
	mvalue.Table = tb
	mvalue.Type = sqlType
	mvalue.Ts = timestamp
	mvalue.Xid = eventIdx
	mvalue.Position = posStr
	mvalue.Data = _genMaxwellValueOfData(sqlType, rEv, colDefs)
	mvalue.Old = _genMaxwellValueOfOld(sqlType, rEv, colDefs)
	mvalue.Commit = false
	mvalue.Xoffset = 0
	return mvalue
}

func DoKafkaPublish(chmaxwell chan kafka.Message, wg *sync.WaitGroup, netAddrs []string) {
	defer wg.Done()
	w := &kafka.Writer{
		Addr: kafka.TCP(netAddrs...),
		// NOTE: When Topic is not defined here, each Message must define it instead.
		Balancer:               &kafka.LeastBytes{},
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
	}

	messages := make([]kafka.Message, 0, 32)
	var err error
	var chCloseed bool = false
	var m kafka.Message

	tk := time.Tick(5 * time.Second)
	for {
		select {
		case <-tk:
			if len(messages) > 0 {
				if err = w.WriteMessages(context.Background(), messages[0:]...); err != nil {
					log.Fatal("failed to write messages:", err)
					time.Sleep(10 * time.Second)
				}
				messages = nil
				messages = make([]kafka.Message, 0, 32)

			}
			if chCloseed {
				if err := w.Close(); err != nil {
					log.Fatal("failed to close writer:", err)
				}
				break
			}
		case m, chCloseed = <-chmaxwell:
			messages = append(messages, m)
		}

	}

}
