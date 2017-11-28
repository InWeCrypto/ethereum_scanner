package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/urfave/cli"

	"github.com/Jeffail/gabs"
	"github.com/lib/pq"
)

const (
	Token    = 1 // 代币
	transfer = "0xa9059cbb"
)

const (
	methodGetBlockByNumber       = "eth_getBlockByNumber"
	methodGetBlockNumber         = "eth_blockNumber"
	methodeGetTransactionReceipt = "eth_getTransactionReceipt"
)

type Transaction struct {
	Timestamp   big.Int // 交易时间
	BlockNumber big.Int // 区块号
	TokenValue  big.Int // 代币数量
	Gas         big.Int // 手续费
	UsedGas     big.Int
	Value       big.Int // eth number
	Hash        string  // tx id
	From        string  //  发起者
	To          string  // 接受者（合约地址）
	AddrToken   string  //  代币地址
	Type        int     // 类型 1 表示是代币 0 表示Eth
	MethodId    string
}

// 解析交易
func parseTransactions(bts []byte) ([]Transaction, error) {
	var ret []Transaction
	jsonParsed, err := gabs.ParseJSON(bts)
	if value, ok := jsonParsed.Path("error.code").Data().(float64); ok && value > 0 {
		msg, _ := jsonParsed.Path("error.message").Data().(string)
		return nil, errors.New(msg)
	}

	count, err := jsonParsed.ArrayCount("result", "transactions")
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	var timestamp big.Int
	var blocknumber big.Int
	t, _ := jsonParsed.Path("result.timestamp").Data().(string)
	r, _ := jsonParsed.Path("result.number").Data().(string)

	timestamp.UnmarshalJSON([]byte(t))
	blocknumber.UnmarshalJSON([]byte(r))
	children, _ := jsonParsed.S("result", "transactions").Children()
	for _, child := range children {
		// 获取数据 保存到数据库中
		var tx Transaction
		tx.Timestamp = timestamp
		tx.BlockNumber = blocknumber
		tx.From, _ = child.Path("from").Data().(string)
		tx.To, _ = child.Path("to").Data().(string)
		tx.Hash, _ = child.Path("hash").Data().(string)
		gas := child.Path("gas").Data().(string)
		value := child.Path("value").Data().(string)
		tx.Gas.UnmarshalJSON([]byte(gas))
		tx.Value.UnmarshalJSON([]byte(value))
		input, _ := child.Path("input").Data().(string)
		// Function: transfer(address _to, uint256 _value)
		// MethodID: 0xa9059cbb
		// 0xa9059cbb000000000000000000000000
		// [0]:00000000000000000000000075186ece18d7051afb9c1aee85170c0deda23d82
		// [1]:0000000000000000000000000000000000000000000000364db9fbe6a7902000
		if len(input) > 74 && string(input[:10]) == transfer {
			//			log.Println(string(input))
			tx.MethodId = string(input[:10])

			tx.AddrToken = string(append([]byte{'0', 'x'}, input[34:74]...))
			tx.TokenValue.UnmarshalJSON(append([]byte{'0', 'x'}, input[74:]...))
			tx.Type = Token
		}

		child.Set(t, "timestamp")
		ret = append(ret, tx)
	}
	return ret, nil
}

var emptyRequest Request

func txBathInsert(txn *sql.Tx, tblname string, transactions []Transaction) error {
	// 事务
	stmt, err := txn.Prepare(pq.CopyIn(tblname,
		"ttimestamp",
		"blocknumber",
		"addr_from",
		"addr_to",
		"gas",
		"value",
		"hash",
		"type",
		"method_id",
		"addr_token",
		"token_value",
		"gas_used"))
	if err != nil {
		return err
	}

	for _, v := range transactions {
		_, err = stmt.Exec(v.Timestamp.Int64(),
			v.BlockNumber.String(),
			v.From,
			v.To,
			v.Gas.String(),
			v.Value.String(),
			v.Hash,
			v.Type,
			v.MethodId,
			v.AddrToken,
			v.TokenValue.String(),
			v.UsedGas.String())
		if err != nil {
			return err
		}
	}

	if _, err := stmt.Exec(); err != nil {
		return err
	}

	return stmt.Close()
}

func insert(db *sql.DB, tblname string, transactions []Transaction) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	if err := txBathInsert(txn, tblname, transactions); err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

func getBlockNumber(addr string) (*big.Int, error) {
	var ret = big.NewInt(0)
	var buff bytes.Buffer
	err := json.NewEncoder(&buff).Encode(NewReq("2.0", methodGetBlockNumber))
	checkErr(err)
	resp, err := http.Post(addr, "application/json", &buff)
	if err != nil {
		return ret, err
	}
	defer resp.Body.Close()
	jsonParsed, _ := gabs.ParseJSONBuffer(resp.Body)

	r, _ := jsonParsed.Path("result").Data().(string)
	ret.UnmarshalJSON([]byte(r))
	return ret, nil
}

func getTransactionReceipt(addr string, request io.Reader) (string, error) {
	resp, err := http.Post(addr, "application/json", request)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	jsonParsed, _ := gabs.ParseJSONBuffer(resp.Body)
	r, _ := jsonParsed.Path("result.gasUsed").Data().(string)
	return r, nil
}

func getTransaction(addr string, request io.Reader) ([]byte, error) {
	resp, err := http.Post(addr, "application/json", request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// 数据读出来序列化单独处理
	return ioutil.ReadAll(resp.Body)
}

type Request struct {
	Jsonrcp string
	Method  string
	Params  []interface{}
	Id      int
}

// cache request
func NewReq(jsonrcp string, method string, param ...interface{}) *Request {
	r := new(Request)
	r.Jsonrcp = jsonrcp
	r.Method = method
	r.Params = append(r.Params, param...)
	return r
}

func workerParse(wg *sync.WaitGroup, ch <-chan []byte, next chan<- []Transaction) {
	defer wg.Done()
	defer close(next)
	for bts := range ch {
		transactions, err := parseTransactions(bts)
		if err != nil {
			log.Println("parse transaction error:", err, string(bts))
		}

		if len(transactions) > 0 {
			next <- transactions
		}
	}
}

func workerSave(wg *sync.WaitGroup, db *sql.DB, tblname string, ch <-chan []Transaction, next chan<- []Transaction) {
	defer wg.Done()
	defer close(next)
	for transactions := range ch {
		err := insert(db, tblname, transactions)
		// 重新执行
		if err != nil {
			if strings.Contains(err.Error(), "ethtx_pkey") {
				log.Println("insert duplicate key:", err)
				UpdsertTransactions(db, tblname, transactions)
			} else {
				// WARNING
				// 忽略错误可能或漏掉数据
				log.Println("insert error:", err)
				continue
			}
		}
		next <- transactions
	}
}

func UpdsertTransactions(db *sql.DB, tblname string, transactions []Transaction) {
	txn, err := db.Begin()
	if err != nil {
		log.Println("begin tx error", err)
		return
	}

	sql := "delete from %v where hash in ( "
	size := len(transactions) - 1
	for i, v := range transactions {
		sql += fmt.Sprintf(`'%v'`, v.Hash)
		if i != size {
			sql += ","
		}
	}
	sql += ")"
	sql = fmt.Sprintf(sql, tblname)

	_, err = txn.Exec(sql)
	if err != nil {
		log.Println(" sql ", sql, " err ", err)
	}

	if err != nil {
		txn.Rollback()
		return
	}

	if err := txBathInsert(txn, tblname, transactions); err != nil {
		txn.Rollback()
		log.Println("insert  error", err)
		return
	}
	txn.Commit()
}

func workerPush(wg *sync.WaitGroup, ch <-chan []Transaction, push func(string), statCH chan<- *Transaction) {
	defer wg.Done()
	for transactions := range ch {
		for _, v := range transactions {
			push(fmt.Sprintf(`{"hash":"%v"}`, v.Hash))
			// 通知统计模块有新的数据插入了是否需要统计
			statCH <- &v
		}
	}
}

func workerGetGasUsed(wg *sync.WaitGroup, addr string, ch <-chan []Transaction, next chan<- []Transaction) {
	defer wg.Done()
	defer close(next)
	for txs := range ch {
		for i := range txs {
			r := NewReq("2.0", methodeGetTransactionReceipt, txs[i].Hash)
			var buff bytes.Buffer
			json.NewEncoder(&buff).Encode(r)
		L:
			used, err := getTransactionReceipt(addr, &buff)
			if err != nil {
				log.Println(err)
				goto L
			}
			txs[i].UsedGas.UnmarshalJSON([]byte(used))
		}

		next <- txs
	}
}

func workerGet(wg *sync.WaitGroup, addr string, ch <-chan int64, next chan<- []byte) {
	defer wg.Done()
	defer close(next)
	for id := range ch {
		hex := fmt.Sprintf("0x%x", id)
		r := NewReq("2.0", methodGetBlockByNumber, hex, true)
		var buff bytes.Buffer
		json.NewEncoder(&buff).Encode(r)
	L:
		bts, err := getTransaction(addr, &buff)
		if err != nil {
			log.Println(err)
			goto L
		}
		next <- bts
	}
}

func moniter(addr string, from *big.Int, ch chan<- int64, dur time.Duration) {
	defer close(ch)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGTERM, syscall.SIGHUP)
	tick := time.NewTicker(dur)
	var to = big.NewInt(0)
	var err error
	for {
		select {
		case sig := <-shutdown:
			log.Println("sig", sig)
			return
		case <-tick.C:
			to, err = getBlockNumber(addr)
			// log.Println(fmt.Sprintf("get last block %d %s",to,addr))
			if err != nil {
				log.Println(err)
				continue
			}
		default:
			diff := to.Int64() - from.Int64() + 1

			// log.Println(fmt.Sprintf("block diff %d", diff))

			// 相差1个块
			if diff <= 0 {
				goto SLEEP
			}

			for x := int64(1); x <= diff; x++ {
				select {
				case ch <- from.Add(from, big.NewInt(1)).Int64():
				default:
					goto SLEEP
				}
			}
			continue
		SLEEP:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func getMaxBlockNumber(db *sql.DB, tblname string) (*big.Int, error) {

	tblname = pq.QuoteIdentifier(tblname)
	rows, err := db.Query(fmt.Sprintf(`SELECT  count(*) from (SELECT * from %s LIMIT 1) as tmp;`, tblname))
	if err != nil {
		return nil, err
	}

	var cnt int64
	for rows.Next() {
		if err = rows.Scan(&cnt); err != nil {
			return nil, err
		}
	}

	if cnt == 0 {
		return big.NewInt(0), nil
	}

	rows, err = db.Query(fmt.Sprintf(`select max(blocknumber)  from  %s;`, tblname))
	if err != nil {
		return nil, err
	}

	var bts []byte
	for rows.Next() {
		if err = rows.Scan(&bts); err != nil {
			return nil, err
		}
	}

	x := big.NewInt(0)
	err = x.UnmarshalJSON(bts)
	return x, err
}

var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	/*
		log.Printf("TOPIC: %s\n", msg.Topic())
		log.Printf("MSG: %s\n", msg.Payload())
	*/
}

func main() {
	app := &cli.App{
		Name: "ethtx",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "drivername",
				Value: "postgres",
				Usage: "database type default postgres",
			},
			&cli.StringFlag{
				Name:  "dbport",
				Value: "5432",
				Usage: "db port ",
			},
			&cli.StringFlag{
				Name:  "dbname",
				Value: "postgres",
				Usage: "database name default postgres",
			},
			&cli.StringFlag{
				Name:  "dbusername",
				Value: "postgres",
				Usage: "database user name",
			},
			&cli.StringFlag{
				Name:  "dbpassword",
				Value: "postgres",
				Usage: "database password of username",
			},
			&cli.StringFlag{
				Name:  "dbhost",
				Value: "localhost",
				Usage: "database host",
			},
			&cli.StringFlag{
				Name:  "ethaddr",
				Value: "http://120.77.208.222:8545",
				Usage: "eth request addr",
			},
			&cli.DurationFlag{
				Name:  "mqkeepalive",
				Value: 10 * time.Second,
				Usage: "mq keepalive",
			},
			&cli.StringFlag{
				Name:  "groupid",
				Value: "GID-ETH-TX",
				Usage: "mq client id",
			},

			&cli.StringFlag{
				Name:  "mqhost",
				Value: "tcp://YOUR-ID.mqtt.aliyuncs.com:1883",
				Usage: "host",
			},
			&cli.StringFlag{
				Name:  "mqusername",
				Value: "USERNAME",
				Usage: "username",
			},
			&cli.StringFlag{
				Name:  "mqpassword",
				Value: "PASSWORD",
				Usage: "password",
			},
			&cli.StringFlag{
				Name:  "mqtopic",
				Value: "ethtx",
				Usage: "topic",
			},
			&cli.StringFlag{
				Name:  "tablename",
				Value: "ethtx",
				Usage: "postgres table name",
			},
			&cli.StringFlag{
				Name:  "stat_tablename",
				Value: "eth_statistics",
				Usage: "postgres stat table name",
			},
			&cli.IntFlag{
				Name:  "mqpushqos",
				Value: 0,
				Usage: "mq push qos",
			},
			&cli.IntFlag{
				Name:  "workerqsize",
				Value: 4096,
				Usage: "mq push qos",
			},
			&cli.DurationFlag{
				Name:  "moniter-period",
				Value: 200 * time.Millisecond,
				Usage: "moniter period",
			},
		},
		Action: func(c *cli.Context) error {
			// db config
			drivername := c.String("drivername")
			dbusername := c.String("dbusername")
			dbname := c.String("dbname")
			dbpassword := c.String("dbpassword")
			dbhost := c.String("dbhost")
			dbport := c.String("dbport")
			// mq config
			groupid := c.String("groupid")
			mqhost := c.String("mqhost")
			mqusername := c.String("mqusername")
			mqpassword := c.String("mqpassword")
			mqtopic := c.String("mqtopic")
			mqpushqos := c.Int("mqpushqos")
			mqkeepalive := c.Duration("mqkeepalive")
			qsize := c.Int("workerqsize")
			// other
			ethaddr := c.String("ethaddr")
			workers := c.Int("worker-size")
			moniterperiod := c.Duration("moniter-period")
			tblname := c.String("tablename")
			// 			stat_tablename := c.String("stat_tablename")
			log.Println("driver:", drivername)
			log.Println("dbname:", dbname)
			log.Println("dbpassword:", dbpassword)
			log.Println("ethaddr:", ethaddr)
			log.Println("workers:", workers)
			log.Println("dbport:", dbport)
			log.Println("dbusername:", dbusername)
			log.Println("moniter-period:", moniterperiod)

			mac := hmac.New(sha1.New, []byte(mqpassword))
			mac.Write([]byte(groupid))
			hmac_password := base64.StdEncoding.EncodeToString(mac.Sum(nil))

			opts := MQTT.NewClientOptions().AddBroker(mqhost)
			opts.SetDefaultPublishHandler(f)
			opts.SetUsername(mqusername)
			opts.SetPassword(hmac_password)
			opts.SetKeepAlive(mqkeepalive)
			opts.SetClientID(fmt.Sprintf("%v@@@%v", groupid, time.Now().Nanosecond()))
			log.Println(opts)
			// 可以改成重连接池中获取 提高并法度
			client := MQTT.NewClient(opts)
			token := client.Connect()
			token.Wait()
			checkErr(token.Error())
			token = client.Subscribe(mqtopic, 0, nil)
			token.Wait()
			checkErr(token.Error())
			// push message
			push := func(json string) {
				// 失败最多尝试10次
				for i := 0; i < 10; i++ {
					if token := client.Publish(mqtopic, byte(mqpushqos), false, json); token.Wait() && token.Error() != nil {
						log.Println("pushlish error", token.Error())
						time.Sleep(100 * time.Millisecond)
					} else {
						return
					}
				}
			}

			db, err := sql.Open(drivername, fmt.Sprintf("user=%v password=%v host=%v dbname=%v port=%v sslmode=disable", dbusername, dbpassword, dbhost, dbname, dbport))
			checkErr(err)
			from, err := getMaxBlockNumber(db, tblname)
			checkErr(err)
			// 定义流水线流程
			log.Println("current block number", from)
			ch := make(chan int64, qsize)
			chParse := make(chan []byte, qsize)
			chGetGas := make(chan []Transaction, qsize)
			chSave := make(chan []Transaction, qsize)
			chPush := make(chan []Transaction, qsize)
			statCH := make(chan *Transaction, qsize)
			var wg = &sync.WaitGroup{}

			wg.Add(1)
			go workerGet(wg, ethaddr, ch, chParse)
			wg.Add(1)
			go workerParse(wg, chParse, chGetGas)
			wg.Add(1)
			go workerGetGasUsed(wg, ethaddr, chGetGas, chSave)
			wg.Add(1)
			go workerSave(wg, db, tblname, chSave, chPush)
			wg.Add(1)
			go workerPush(wg, chPush, push, statCH)

			// 30s 打印一次统计数据
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				for range ticker.C {
					log.Println("ch size:", len(ch), " get next chan size:", len(chParse), " parse next chan size:", len(chGetGas), "gase used next ch size:", len(chSave), " save next ch size:", len(chPush), " push next chan size:", len(statCH), " max ch:", qsize)

				}
			}()

			//var moniterMin = make(chan *Transaction, qsize)
			//var moniter5Min = make(chan *Transaction, qsize)
			//var moniter10Min = make(chan *Transaction, qsize)
			//var moniter30Min = make(chan *Transaction, qsize)
			//  通知statmonitor
			//go func(outs ...chan<- *Transaction) {
			//	for v := range statCH {
			//		for _, ch := range outs {
			//			ch <- v
			//		}
			//	}
			//}(moniterMin, moniter5Min, moniter10Min, moniter30Min)

			//quit := make(chan bool)
			//defer close(quit)
			// 1分钟
			//go statMoniter(db, 60, tblname, stat_tablename+"_1min", quit, moniterMin)

			// 5分钟
			//go statMoniter(db, 60*5, tblname, stat_tablename+"_5min", quit, moniter5Min)

			// 10分钟
			//go statMoniter(db, 60*10, tblname, stat_tablename+"_10min", quit, moniter10Min)

			// 30分钟
			//go statMoniter(db, 60*30, tblname, stat_tablename+"_30min", quit, moniter30Min)
			moniter(ethaddr, from, ch, moniterperiod)
			wg.Wait()
			log.Printf("shutdown success ...")
			return nil
		},
	}
	app.Run(os.Args)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
