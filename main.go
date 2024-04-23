package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	bolt "go.etcd.io/bbolt"
)

type Data struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	// Define a context
	ctx := context.Background()

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Connect to BoltDB
	db, err := bolt.Open("data.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Start a goroutine to flush data to BoltDB
	go flushToBoltDB(ctx, redisClient, db)

	// Simulate writing data every 2 seconds
	go func() {
		counter := 1
		for {
			// Example write data
			data := Data{Key: fmt.Sprintf("key %v", counter), Value: fmt.Sprintf("value %v", counter)}
			counter++
			writeDataToRedis(ctx, redisClient, data)
			time.Sleep(2 * time.Second)
		}
	}()

	// Wait indefinitely
	select {}
}

func writeDataToRedis(ctx context.Context, redisClient *redis.Client, data Data) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling data:", err)
		return
	}

	log.Println("jsonData", string(jsonData))

	err = redisClient.LPush(ctx, "write_queue", string(jsonData)).Err()
	if err != nil {
		fmt.Println("Error pushing data to Redis:", err)
	}
}

func flushToBoltDB(ctx context.Context, redisClient *redis.Client, db *bolt.DB) {
	for {
		select {
		case <-ctx.Done():
			return // Exit if context is cancelled
		default:
			// Pop data from Redis queue
			data, err := redisClient.RPop(ctx, "write_queue").Result()
			if err != nil {
				if err != redis.Nil {
					fmt.Println("Error popping data from Redis:", err)
				}
				continue
			}

			if data != "" {
				// Unmarshal JSON data
				var d Data
				err := json.Unmarshal([]byte(data), &d)
				if err != nil {
					fmt.Println("Error unmarshaling data:", err)
					continue
				}

				// Write data to BoltDB
				err = db.Update(func(tx *bolt.Tx) error {
					bucket, err := tx.CreateBucketIfNotExists([]byte("data"))
					if err != nil {
						return err
					}
					return bucket.Put([]byte(d.Key), []byte(d.Value))
				})
				if err != nil {
					fmt.Println("Error writing data to BoltDB:", err)
				}

				db.View(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte("data"))
					if bucket == nil {
						return nil
					}
					bucket.ForEach(func(k, v []byte) error {
						fmt.Printf("Key: %s, Value: %s\n", k, v)
						return nil
					})
					return nil
				})
			} else {
				// Sleep if no data available in the queue
				time.Sleep(1 * time.Second)
			}
		}
	}
}
