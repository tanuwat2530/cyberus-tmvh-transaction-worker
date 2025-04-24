package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	// Start background worker in a goroutine
	go backgroundWorker()
	// Keep main running so the program doesn't exit
	select {} // This blocks forever
}

// This is the background job that runs forever
func backgroundWorker() {

	var ctx = context.Background()
	var WAIT_INTERVAL = (27 * time.Second)
	var cursor uint64 = 0
	var matchPattern = "tmvh-transaction-callback-api:*" // Pattern to match keys
	var count = int64(100)                               // Limit to 100 keys per scan

	fmt.Println("##### TMVH TRANSACTION WORKER RUNNING #####")
	//config redis pool
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Change if needed
		Password: "",               // No password by default
		DB:       0,                // Default DB
		PoolSize: 100,              //Connection pools
	})

	//config database pool
	dsn := "host=localhost user=root password=11111111 dbname=cyberus_db port=5432 sslmode=disable TimeZone=Asia/Bangkok search_path=root@cyberus"
	db, errDatabase := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if errDatabase != nil {
		log.Fatal("Failed to connect to database:", errDatabase)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Failed to get generic database object:", err)
	}
	// Set connection pool settings
	sqlDB.SetMaxOpenConns(100)                 // Maximum number of open connections
	sqlDB.SetMaxIdleConns(10)                  // Maximum number of idle connections
	sqlDB.SetConnMaxLifetime(10 * time.Minute) // Connection max lifetime

	var wg sync.WaitGroup

	for {

		// Perform the scan with the match pattern and count
		keys, newCursor, err := rdb.Scan(ctx, cursor, matchPattern, count).Result()
		if err != nil {
			panic(err)
		}
		if len(keys) > 0 {
			//fmt.Printf("number of key : %d\n", len(keys))
			for i := 0; i < len(keys); i++ {
				//fmt.Printf("key[%d] : ", i)
				//fmt.Println(keys[i])

				//fmt.Println("Send Key to Worker : ", keys[i]) // Print only the first key
				// Example: Get the value of the key (assuming it's a string)
				valJson, err := rdb.Get(ctx, keys[i]).Result()
				if err != nil {
					log.Fatal("Error getting value : ", err)
				} else {
					// Start multiple goroutines (threads)
					wg.Add(1)
					go threadWorker(i, &wg, valJson, rdb, ctx, db)
				}
			}
		}
		// Update cursor for the next iteration
		cursor = newCursor
		// If the cursor is 0, then the scan is complete
		if cursor == 0 {
			fmt.Println("Wait for next scan")
			time.Sleep(WAIT_INTERVAL)
			//break
		}
		wg.Wait() // Block here until all goroutines call Done()
	}
	//}

}

// Function that simulates work for a thread
func threadWorker(id int, wg *sync.WaitGroup, jsonString string, rdb *redis.Client, ctx context.Context, db *gorm.DB) error {

	type TransactionData struct {
		Code         string `json:"code"`
		Desc         string `json:"desc"`
		Msisdn       string `json:"msisdn"`
		Operator     string `json:"operator"`
		Shortcode    string `json:"short-code"`
		TranRef      string `json:"tran-ref"`
		Timestamp    int    `json:"timestamp"`
		ReturnStatus string `json:"cyberus-return"`
	}

	//Table name on database
	type tmvh_transaction_logs struct {
		ID            string `gorm:"primaryKey"`
		Code          string `gorm:"column:code"`
		Description   string `gorm:"column:description"`
		Msisdn        string `gorm:"column:msisdn"`
		Operator      string `gorm:"column:operator"`
		ShortCode     string `gorm:"column:short_code"`
		TranRef       string `gorm:"column:tran_ref"`
		Timestamp     int64  `gorm:"column:timestamp"`
		CyberusReturn string `gorm:"column:cyberus_return"`
	}
	//	fmt.Printf("Worker No : %d\n start", id)

	// Convert struct to JSON string
	var transactionData TransactionData
	errTransactionnData := json.Unmarshal([]byte(jsonString), &transactionData)
	if errTransactionnData != nil {
		fmt.Println("JSON Marshal error : ", errTransactionnData)
		return fmt.Errorf("JSON DECODE ERROR : " + errTransactionnData.Error())
	}

	// // Print the data to the console
	//fmt.Println("##### Insert into Database #####")
	//fmt.Println("Msisdn : " + transactionData.Msisdn)
	// fmt.Println("Shortcode : " + transactionData.Shortcode)
	// fmt.Println("Operator  : " + transactionData.Operator)
	// fmt.Println("Action  : " + transactionData.Action)
	// fmt.Println("Code  : " + transactionData.Code)
	// fmt.Println("Desc  : " + transactionData.Desc)
	//fmt.Println("Timestamp  : " + strconv.FormatInt(int64(transactionData.Timestamp)))
	// fmt.Println("TranRef  : " + transactionData.TranRef)
	// fmt.Println("Action  : " + transactionData.Action)
	// fmt.Println("RefId  : " + transactionData.RefId)
	// fmt.Println("Media  : " + transactionData.Media)
	// fmt.Println("Token  : " + transactionData.Token)
	// fmt.Println("CyberusReturn  : " + transactionData.ReturnStatus)

	//defer wg.Done() // Mark this goroutine as done when it exits

	logEntry := tmvh_transaction_logs{
		ID:            transactionData.TranRef,
		Code:          transactionData.Code,
		Description:   transactionData.Desc,
		Msisdn:        transactionData.Msisdn,
		Operator:      transactionData.Operator,
		ShortCode:     transactionData.Shortcode,
		Timestamp:     int64(transactionData.Timestamp),
		TranRef:       transactionData.TranRef,
		CyberusReturn: transactionData.ReturnStatus,
	}

	if errInsertDB := db.Create(&logEntry).Error; errInsertDB != nil {
		fmt.Println("ERROR INSERT : " + errInsertDB.Error())
		return fmt.Errorf(errInsertDB.Error())
	}

	redis_set_key := "tmvh-transaction-log-worker:" + transactionData.TranRef
	ttl := 240 * time.Hour // expires in 10 day
	// Set key with TTL
	errSetRedis := rdb.Set(ctx, redis_set_key, jsonString, ttl).Err()
	if errSetRedis != nil {
		fmt.Println("Redis SET error:", errSetRedis)
		return fmt.Errorf("REDIS SET ERROR : " + errSetRedis.Error())
	}

	redis_del_key := "tmvh-transaction-callback-api:" + transactionData.TranRef
	rdb.Del(ctx, redis_del_key).Result()

	wg.Done()
	fmt.Printf("Transaction worker No : %d finished\n", id)
	return nil
}
