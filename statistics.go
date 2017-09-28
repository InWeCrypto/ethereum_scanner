package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"
)

// 统计交易量
// 以太坊和合约
// 可能同时存在合约和转账行为
// tokenvaue 为代币数量
// 所以统计因该 group by addr_token && type
// sum(value)
// sum(tokenvaue)

func statMoniter(db *sql.DB, interval int64, tblname, stat_tblname string, quit chan bool, in <-chan *Transaction) {
	now := time.Now().Unix()
	start := queryLastTime(db, stat_tblname)
	if start == 0 {
		for {
			start = queryMinTime(db, tblname)
			if start > 0 {
				start = start - start%interval
				break
			}
			time.Sleep(30 * time.Second)
		}
	}

	// 统计中间漏掉的数据
	passStart := start
	if now-start >= interval {
		go func(s int64) {
			for now-s > interval {
				statSQL(db, tblname, stat_tblname, s, s+interval)

				s += interval
				time.Sleep(1 * time.Second)

				//				if count > 0 {
				//					log.Printf("pass: %+v - %+v  :  %+v", s, s+interval, count)
				//				}
			}
		}(start)

		// 重新开始
		start = now - now%interval
	}
	passEnd := start

	// 距离下一次统计还有多长时间
	du := int64(0)
	if now < start {
		du = interval
	} else {
		du = interval - (now - start)
	}

	// 多等待10秒 eg:00:01:10 这一刻统计 00:00:00-00:01:00 时间段的数据
	time.Sleep(10 * time.Second)

	for {
		select {
		case <-quit:
			log.Println("quit")
			return
		case tran := <-in:
			// 新的交易数据错过了上一个统计间隔
			tranTime := tran.Timestamp.Int64()
			if tranTime < start && tran.To != "" {
				if tranTime < passStart || tranTime >= passEnd {
					updateSQL(db, stat_tblname, tran, interval)
					//					log.Printf("miss: %+v :  %+v", start, tranTime)
				}
			}

		case <-time.After(time.Second * time.Duration(du)):

			statSQL(db, tblname, stat_tblname, start, start+interval)

			du = interval
			start += interval

			//			if count > 0 {
			//				log.Printf("stat: %+v - %+v  :  %+v", start, start+interval, count)
			//			}
		}
	}
}

// 纠正数据
func updateSQL(db *sql.DB, stat_tblname string, tran *Transaction, interval int64) error {
	if tran == nil {
		return nil
	}

	// 查找该时间段内是否已经有统计记录
	sqlStr := `select e.id from ` + stat_tblname + ` e 
				where e.start_time <= $1 and e.end_time > $2 and e.type = $3 and e.interval = $4 
                  and e.addr_token = '` + tran.AddrToken + `' and e.addr_to = '` + tran.To + `'`

	var id int64
	err := db.QueryRow(sqlStr, tran.Timestamp.Int64(), tran.Timestamp.Int64(), tran.Type, interval).Scan(&id)
	if err != nil {
		log.Println("select ", err)
	}

	// 没有对应统计间隔的记录，则新加
	if err == sql.ErrNoRows {
		sqlStr = `insert into ` + stat_tblname + `("start_time","end_time","sum_value","sum_token_value","type","addr_token","addr_to","sum_gas","interval")
		VALUES `

		start := tran.Timestamp.Int64() - tran.Timestamp.Int64()%interval
		end := start + interval
		sqlStr += fmt.Sprintf("(%+v,%+v,%+v,%+v,%+v,'%+v','%+v',%+v,%+v);",
			start, end, tran.Value.String(), tran.TokenValue.String(), tran.Type, tran.AddrToken, tran.To, tran.Gas.String(), interval)

		r, err := db.Exec(sqlStr)
		if err != nil {
			log.Println("insert ", err)
		} else {
			count, err := r.RowsAffected()
			if err != nil {
				log.Println("insert ", err)
			} else {
				if count > 0 {
					log.Println("new insert count:", count)
				}
			}
		}
	}

	// 如果已经有数据，则直接累加
	if id != 0 {
		sqlStr = `update ` + stat_tblname + ` set sum_value = sum_value + ` + tran.Value.String() + `,
										 	   sum_token_value = sum_token_value + ` + tran.TokenValue.String() + `,
											   sum_gas = sum_gas + ` + tran.Gas.String() + `
	 										   where id = $1`

		r, err := db.Exec(sqlStr, id)
		if err != nil {
			log.Println("update ", err)
			return err
		}

		_, err = r.RowsAffected()
		if err != nil {
			log.Println("update ", err)
			return err
		}

		//		if count > 0 {
		//			log.Println("update count:", count)
		//		}
	}

	return nil
}

// 分组统计
func statSQL(db *sql.DB, tblname, stat_tblname string, start, end int64) (int64, error) {
	if end <= start {
		return 0, errors.New("end less than start")
	}

	interval := end - start

	sql := `INSERT INTO ` + stat_tblname + `("start_time","end_time","sum_value","sum_token_value","type","addr_token","addr_to","sum_gas","interval")
				select $1,$2 ,sum(e.value),sum(e.token_value),e.type,e.addr_token,e.addr_to,sum(e.gas),$3 from ` + tblname + `  e 
    			 	where e.ttimestamp >= $4 and e.ttimestamp < $5 and e.addr_to != '' group by e.type, e.addr_to,e.addr_token`

	r, err := db.Exec(sql, start, end, interval, start, end)
	if err != nil {
		log.Println("stat ", sql, err)
		return 0, err
	}

	count, err := r.RowsAffected()
	if err != nil {
		log.Println("stat ", sql, err)
		return 0, err
	}

	return count, nil
}

func queryMinTime(db *sql.DB, tblname string) int64 {
	var tmp int64
	err := db.QueryRow(fmt.Sprintf(`select min(ttimestamp)  from %s;`, tblname)).Scan(&tmp)
	if err != nil {
		log.Println("queryMinTime:", err)
		return 0
	}

	return tmp
}

// 最后一次更新时间
func queryLastTime(db *sql.DB, tblname string) int64 {
	var tmp int64
	err := db.QueryRow(fmt.Sprintf(`select max(end_time)  from %s;`, tblname)).Scan(&tmp)
	if err != nil {
		log.Println("queryLastTime:", err)
		return 0
	}

	return tmp
}
