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

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// main sets up the database and Redis connections and starts the background worker.
func main() {
	// It's better to load these from environment variables for security and flexibility.
	redisConnection := os.Getenv("BN_REDIS_URL")
	dbConnection := os.Getenv("BN_DB_URL")

	if redisConnection == "" || dbConnection == "" {
		log.Fatal("BN_REDIS_URL and BN_DB_URL environment variables must be set.")
	}

	// Configure Redis client with a connection pool.
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisConnection,
		Password: "",  // No password by default
		DB:       0,   // Default DB
		PoolSize: 100, // Connection pools
	})

	// Configure database client with a connection pool.
	db, errDatabase := gorm.Open(postgres.Open(dbConnection), &gorm.Config{})
	if errDatabase != nil {
		log.Fatal("Failed to connect to database:", errDatabase)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Failed to get generic database object:", err)
	}

	// Set connection pool settings for the database.
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	// Start the background worker in a separate goroutine.
	go backgroundWorker(rdb, db)

	log.Println("Application started successfully. Background worker is running.")
	// Keep the main function running indefinitely so the background goroutine can live.
	select {}
}

// backgroundWorker continuously scans Redis for jobs and dispatches them to worker goroutines.
func backgroundWorker(rdb *redis.Client, db *gorm.DB) {
	var ctx = context.Background()
	const WAIT_INTERVAL = 17 * time.Second // Reduced wait time for more responsive scanning
	var cursor uint64 = 0
	const matchPattern = "tmvh-transaction-callback-api:*"
	const count = int64(100)

	log.Println("##### TMVH TRANSACTION WORKER RUNNING #####")

	var wg sync.WaitGroup

	for {
		// Perform the Redis scan.
		keys, newCursor, err := rdb.Scan(ctx, cursor, matchPattern, count).Result()
		if err != nil {
			// FIX: Instead of panicking, log the error and wait before retrying.
			// This makes the worker resilient to temporary Redis connection issues.
			log.Printf("ERROR scanning Redis: %v. Retrying in %s", err, WAIT_INTERVAL)
			time.Sleep(WAIT_INTERVAL)
			continue // Continue to the next loop iteration.
		}

		if len(keys) > 0 {
			log.Printf("Found %d keys to process in this batch.", len(keys))
			for i, key := range keys {
				valJson, err := rdb.Get(ctx, key).Result()
				if err != nil {
					// FIX: Instead of log.Fatal, log the error and skip this specific key.
					// This allows the worker to continue with other keys in the batch.
					log.Printf("ERROR getting value for key %s: %v. Skipping.", key, err)
					continue
				}

				wg.Add(1)
				// Pass the original Redis key to the worker for reliable deletion later.
				go threadWorker(i, &wg, valJson, rdb, ctx, db, key)
			}
		}

		cursor = newCursor
		// If the cursor is 0, the scan of the entire keyspace is complete for now.
		if cursor == 0 {
			time.Sleep(WAIT_INTERVAL)
		}

		// Block here until all goroutines in the current batch have called wg.Done().
		wg.Wait()
	}
}

// threadWorker processes a single job from Redis.
func threadWorker(id int, wg *sync.WaitGroup, jsonString string, rdb *redis.Client, ctx context.Context, db *gorm.DB, redisKey string) {
	// FIX: Defer wg.Done() at the top. This is the most critical fix.
	// It guarantees that the WaitGroup is notified that this goroutine has finished,
	// regardless of where the function returns. This prevents the program from hanging.
	defer wg.Done()

	log.Printf("Worker %d: Started processing key: %s", id, redisKey)

	// Struct definitions
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

	type client_services struct {
		ID              uint   `gorm:"column:id;primaryKey"`
		DNURL           string `gorm:"column:dn_url"`
		PostbackURL     string `gorm:"column:postback_url"`
		PostbackCounter int    `gorm:"column:postback_counter"`
	}

	// Unmarshal the primary transaction data. If this fails, we cannot proceed.
	var transactionData TransactionData
	if err := json.Unmarshal([]byte(jsonString), &transactionData); err != nil {
		log.Printf("Worker %d: ERROR - JSON Unmarshal failed for key %s: %v", id, redisKey, err)
		// We will delete the invalid key from Redis to prevent it from being processed again.
		rdb.Del(ctx, redisKey)
		return // Exit this goroutine.
	}

	// --- Optional HTTP Call ---
	var partnerDataEntry client_services
	var telco_operator = "0"
	if transactionData.Operator == "TRUEMOVE" {
		telco_operator = "1"
	} else if transactionData.Operator == "DTAC" {
		telco_operator = "2"
	} else if transactionData.Operator == "AIS" {
		telco_operator = "3"
	}

	queryRes := db.Where("shortcode = ? and telcoid = ?", transactionData.Shortcode, telco_operator).First(&partnerDataEntry)
	if queryRes.Error != nil {
		if queryRes.Error == gorm.ErrRecordNotFound {
			log.Printf("Worker %d: INFO - Partner data not found for key %s. Skipping HTTP call.", id, redisKey)
		} else {
			log.Printf("Worker %d: WARNING - DB query failed for key %s: %v. Skipping HTTP call.", id, redisKey, queryRes.Error)
		}
	} else if partnerDataEntry.DNURL != "" {
		// This block only runs if the DB query was successful and a DNURL exists.
		queryParams := url.Values{}
		queryParams.Add("msisdn", transactionData.Msisdn)
		queryParams.Add("operator", transactionData.Operator)
		queryParams.Add("tran_ref", transactionData.TranRef)
		queryParams.Add("short_code", transactionData.Shortcode)
		queryParams.Add("code", transactionData.Code)
		queryParams.Add("desc", transactionData.Desc)
		queryParams.Add("timestamp", strconv.FormatInt(int64(transactionData.Timestamp), 10))

		paramTargetURL := fmt.Sprintf("%s?%s", partnerDataEntry.DNURL, queryParams.Encode())
		client := http.Client{Timeout: 10 * time.Second}

		resp, err := client.Get(paramTargetURL)
		// FIX: Log HTTP errors but do not stop the worker's main job (DB insert).
		if err != nil {
			log.Printf("Worker %d: WARNING - HTTP GET request failed for key %s: %v", id, redisKey, err)
		} else {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("Worker %d: WARNING - Received non-OK HTTP status for key %s: %s", id, redisKey, resp.Status)
			} else {
				log.Printf("Worker %d: INFO - Successfully sent DN ping for key %s", id, redisKey)
			}
		}
	}

	// --- Critical Database Insert ---
	logEntry := tmvh_transaction_logs{
		ID:            uuid.New().String(),
		Code:          transactionData.Code,
		Description:   transactionData.Desc,
		Msisdn:        transactionData.Msisdn,
		Operator:      transactionData.Operator,
		ShortCode:     transactionData.Shortcode,
		Timestamp:     int64(transactionData.Timestamp),
		TranRef:       transactionData.TranRef,
		CyberusReturn: transactionData.ReturnStatus,
	}

	if err := db.Create(&logEntry).Error; err != nil {
		// FIX: If the main DB insert fails, log it and exit without deleting the Redis key.
		// This allows the job to be picked up and retried on the next scan.
		log.Printf("Worker %d: ERROR - Database insert failed for key %s: %v. Task will be retried.", id, redisKey, err)
		return
	}

	// --- Final Redis Operations ---
	redis_set_key := "tmvh-transaction-log-worker:" + transactionData.TranRef
	ttl := 240 * time.Hour // expires in 10 days
	if err := rdb.Set(ctx, redis_set_key, jsonString, ttl).Err(); err != nil {
		// FIX: Log this error but don't stop. The critical DB work is done.
		log.Printf("Worker %d: WARNING - Redis SET confirmation key failed for key %s: %v", id, redisKey, err)
	}

	// Clean up the original Redis key since the job was processed successfully.
	if err := rdb.Del(ctx, redisKey).Err(); err != nil {
		log.Printf("Worker %d: WARNING - Failed to delete original key %s from Redis: %v", id, redisKey, err)
	}

	log.Printf("Worker %d: Finished processing key %s successfully.", id, redisKey)
}
