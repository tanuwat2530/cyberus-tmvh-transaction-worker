package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	// Start background worker in a goroutine
	redisConnection := os.Getenv("BN_REDIS_URL")
	dbConnection := os.Getenv("BN_DB_URL")
	//config redis pool
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisConnection, // Change if needed
		Password: "",              // No password by default
		DB:       0,               // Default DB
		PoolSize: 100,             //Connection pools
	})

	//config database pool
	db, errDatabase := gorm.Open(postgres.Open(dbConnection), &gorm.Config{})
	if errDatabase != nil {
		log.Fatal("Failed to connect to database:", errDatabase)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Failed to get generic database object:", err)
	}
	// Set connection pool settings
	sqlDB.SetMaxOpenConns(100)                // Maximum number of open connections
	sqlDB.SetMaxIdleConns(10)                 // Maximum number of idle connections
	sqlDB.SetConnMaxLifetime(5 * time.Minute) // Connection max lifetime
	go backgroundWorker(rdb, db)

	// Keep main running so the program doesn't exit
	select {} // This blocks forever
}

// This is the background job that runs forever
func backgroundWorker(rdb *redis.Client, db *gorm.DB) {

	var ctx = context.Background()
	var WAIT_INTERVAL = (27 * time.Second)
	var cursor uint64 = 0
	var matchPattern = "tmvh-transaction-callback-api:*" // Pattern to match keys
	var count = int64(100)                               // Limit to 100 keys per scan

	fmt.Println("##### TMVH TRANSACTION WORKER RUNNING DON'T CLOSE TERMINAL #####")

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
			//fmt.Println("Wait for next scan")
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
		//return fmt.Errorf("JSON DECODE ERROR : " + errTransactionnData.Error())
	}

	type PartnerData struct {
		Id                uint   `json:"id"`
		Keyword           string `json:"keyword"`
		Shortcode         string `json:"shortcode"`
		Telcoid           string `json:"telcoid"`
		Ads_id            string `json:"ads_id"`
		Client_partner_id string `json:"client_partner_id"`
		Wap_aoc_refid     int    `json:"wap_aoc_refid"`
		Wap_aoc_id        string `json:"wap_aoc_id"`
		Wap_aoc_media     int    `json:"wap_aoc_media"`
		Postback_url      string `json:"postback_url"`
		Dn_url            string `json:"dn_url"`
		Postback_counter  int    `json:"postback_counter"`
	}

	//Table name on database
	type client_services struct {
		//ID is the primary key, auto-incremented by the database sequence.
		ID              uint   `gorm:"column:id;primaryKey"`
		Keyword         string `gorm:"column:keyword"`                    // varchar NULL in SQL, using pointer for explicit nullability
		Shortcode       string `gorm:"column:shortcode"`                  // varchar NULL in SQL, using pointer for explicit nullability
		TelcoID         string `gorm:"column:telcoid"`                    // varchar NULL in SQL, using pointer for explicit nullability
		AdsID           string `gorm:"column:ads_id"`                     // varchar NULL in SQL, using pointer for explicit nullability
		ClientPartnerID string `gorm:"column:client_partner_id;not null"` // varchar NOT NULL in SQL
		WapAOCRefID     string `gorm:"column:wap_aoc_refid"`              // varchar NULL in SQL, using pointer for explicit nullability
		WapAOCID        string `gorm:"column:wap_aoc_id"`                 // varchar NULL in SQL, using pointer for explicit nullability
		WapAOCMedia     string `gorm:"column:wap_aoc_media"`              // varchar NULL in SQL, using pointer for explicit nullability
		PostbackURL     string `gorm:"column:postback_url"`               // varchar NULL in SQL, using pointer for explicit nullability
		DNURL           string `gorm:"column:dn_url"`                     // varchar NULL in SQL, using pointer for explicit nullability
		PostbackCounter int    `gorm:"column:postback_counter"`           // int4 NULL in SQL, using pointer for explicit nullability
	}

	var partnerData PartnerData
	errPartnerData := json.Unmarshal([]byte(jsonString), &partnerData)
	if errPartnerData != nil {
		fmt.Println("partnerData : ", errPartnerData)
		//return fmt.Errorf("partnerData : " + errPartnerData.Error())
	}

	partnerDataEntry := client_services{
		DNURL:           partnerData.Dn_url,
		PostbackURL:     partnerData.Postback_url,
		PostbackCounter: int(partnerData.Postback_counter),
	}

	var telco_operator = "0"
	if transactionData.Operator == "TRUEMOVE" {
		telco_operator = "1"
	}
	if transactionData.Operator == "DTAC" {
		telco_operator = "2"
	}
	if transactionData.Operator == "AIS" {
		telco_operator = "3"
	}
	queryRes := db.Where("shortcode = ? and telcoid = ?", transactionData.Shortcode, telco_operator).First(&partnerDataEntry)
	if queryRes.Error != nil {
		if queryRes.Error == gorm.ErrRecordNotFound {
			fmt.Println("not found.")
		} else {
			log.Printf("Error finding : %v", queryRes.Error)
		}
	} else {

		//fmt.Println("DN URL : ", partnerDataEntry.DNURL)
		//fmt.Println("POSTBACK URL : ", partnerDataEntry.PostbackURL)
		//fmt.Println("COUNTER : ", partnerDataEntry.PostbackCounter)
		// Define parameters as a map
		params := map[string]string{
			"msisdn":     transactionData.Msisdn,
			"operator":   transactionData.Operator, // Value with spaces
			"tran_ref":   transactionData.TranRef,  // Another value with spaces and special char
			"short_code": transactionData.Shortcode,
			"code":       transactionData.Code,
			"desc":       transactionData.Desc,
			"timestamp":  strconv.FormatInt(int64(transactionData.Timestamp), 10),
		}
		// Use url.Values to build and URL-encode the query string
		queryParams := url.Values{}
		for key, value := range params {
			queryParams.Add(key, value) // Automatically encodes key and value
		}
		// Construct the full URL with the encoded query string
		paramTargetURL := fmt.Sprintf("%s?%s", partnerDataEntry.DNURL, queryParams.Encode())
		//log.Printf("GET request URL with parameters: %s", paramTargetURL)
		// Create an HTTP client with a timeout
		client := http.Client{
			Timeout: 10 * time.Second, // Set a timeout for the request
		}

		// Make the HTTP GET request
		resp, err := client.Get(paramTargetURL)
		if err != nil {
			fmt.Println("failed to make GET request to ", resp, err)
		}
		defer resp.Body.Close() // Ensure the response body is closed after reading

		// Check the HTTP status code
		if resp.StatusCode != http.StatusOK {
			fmt.Println("received non-OK HTTP status for ", resp, resp.Status)
		}

		// Read the response body
		// bodyBytes, err := ioutil.ReadAll(resp.Body)
		// if err != nil {
		// 	fmt.Println("failed to read response body from", bodyBytes, err)
		// }

	}

	// // Print the data to the console
	// fmt.Println("##### Insert into Database #####")
	// fmt.Println("Msisdn : " + transactionData.Msisdn)
	// fmt.Println("Shortcode : " + transactionData.Shortcode)
	// fmt.Println("Operator  : " + transactionData.Operator)
	// fmt.Println("Action  : " + transactionData.Action)
	// fmt.Println("Code  : " + transactionData.Code)
	// fmt.Println("Desc  : " + transactionData.Desc)
	// fmt.Println("Timestamp  : " + strconv.FormatInt(int64(transactionData.Timestamp)))
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
		redis_del_key := "tmvh-transaction-callback-api:" + transactionData.TranRef
		rdb.Del(ctx, redis_del_key).Result()
		fmt.Println("ERROR INSERT : " + errInsertDB.Error())
		//return fmt.Errorf(errInsertDB.Error())
	}

	redis_set_key := "tmvh-transaction-log-worker:" + transactionData.TranRef
	ttl := 240 * time.Hour // expires in 10 day
	// Set key with TTL
	errSetRedis := rdb.Set(ctx, redis_set_key, jsonString, ttl).Err()
	if errSetRedis != nil {
		fmt.Println("Redis SET error:", errSetRedis)
		//return fmt.Errorf("REDIS SET ERROR : " + errSetRedis.Error())
	}

	redis_del_key := "tmvh-transaction-callback-api:" + transactionData.TranRef
	rdb.Del(ctx, redis_del_key).Result()

	wg.Done()
	fmt.Printf("Transaction worker No : %d finished\n", id)
	return nil
}
