package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

var db *sql.DB
var mqConn *amqp.Connection
var mqChannel *amqp.Channel
var rdb *redis.Client
var ctx = context.Background()

// We will limit traffic to 2 requests per second per domain so it's easy to test
const MaxRequestsPerSecond = 2

func initDB() {
	var err error
	db, err = sql.Open("postgres", "postgres://hookuser:hookpass@localhost:5432/hookforge?sslmode=disable")
	if err != nil { log.Fatal(err) }
}

func initRabbitMQ() {
	var err error
	mqConn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil { log.Fatal(err) }
	mqChannel, err = mqConn.Channel()
	if err != nil { log.Fatal(err) }

	mqChannel.QueueDeclare("webhook_dead", true, false, false, false, nil)
	mqChannel.QueueDeclare("webhook_retry", true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "webhook_jobs",
	})
}

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}
	log.Println("Redis connected.")
}

func main() {
	initDB()
	initRabbitMQ()
	initRedis()
	defer db.Close()
	defer mqChannel.Close()
	defer mqConn.Close()

	msgs, err := mqChannel.Consume("webhook_jobs", "", false, false, false, false, nil)
	if err != nil { log.Fatal(err) }

	httpClient := &http.Client{Timeout: 5 * time.Second}
	log.Println("Worker booted. Rate Limiter Active (2 req/sec).")

	for msg := range msgs {
		eventID := string(msg.Body)

		var targetURL, payload string
		var retryCount int
		err := db.QueryRow("SELECT target_url, payload, retry_count FROM events WHERE id = $1", eventID).Scan(&targetURL, &payload, &retryCount)
		if err != nil {
			msg.Ack(false)
			continue
		}

		// --- RATE LIMITING LOGIC ---
		parsedURL, err := url.Parse(targetURL)
		if err != nil {
			log.Printf("[%s] Invalid URL format, dropping message.", eventID)
			msg.Ack(false)
			continue
		}
		domain := parsedURL.Host
		redisKey := "rate_limit:" + domain

		// Increment the counter for this domain
		requestsThisSecond, err := rdb.Incr(ctx, redisKey).Result()

		// If this is the first request, set the counter to expire in 1 second
		if requestsThisSecond == 1 {
			rdb.Expire(ctx, redisKey, time.Second)
		}

		if requestsThisSecond > MaxRequestsPerSecond {
			log.Printf("🚦 RATE LIMITED: %s is getting too much traffic. Delaying %s...", domain, eventID)

			// Requeue with a short 1-second delay.
			// Notice we DO NOT increment retryCount because this isn't a failure!
			mqChannel.PublishWithContext(ctx, "", "webhook_retry", false, false, amqp.Publishing{
				Expiration: "1000", // 1 second
				Body:       []byte(eventID),
			})
			msg.Ack(false)
			continue // Skip the HTTP request
		}
		// ---------------------------

		log.Printf("Attempting %s (Try %d) -> %s", eventID, retryCount+1, targetURL)
		req, _ := http.NewRequest(http.MethodPost, targetURL, bytes.NewBufferString(payload))
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		isFailure := err != nil || resp.StatusCode >= 500

		if isFailure {
			retryCount++
			if retryCount >= 5 {
				log.Printf("[%s] Max retries reached. Moving to DLQ.", eventID)
				db.Exec("UPDATE events SET status = 'dead', retry_count = $1 WHERE id = $2", retryCount, eventID)
				mqChannel.PublishWithContext(ctx, "", "webhook_dead", false, false, amqp.Publishing{Body: []byte(eventID)})
			} else {
				backoffMs := int(math.Pow(2, float64(retryCount)) * 1000)
				log.Printf("[%s] Failed. Backing off for %d ms...", eventID, backoffMs)
				db.Exec("UPDATE events SET retry_count = $1 WHERE id = $2", retryCount, eventID)
				mqChannel.PublishWithContext(ctx, "", "webhook_retry", false, false, amqp.Publishing{
					Expiration: fmt.Sprintf("%d", backoffMs),
							     Body:       []byte(eventID),
				})
			}
		} else {
			defer resp.Body.Close()
			log.Printf("[%s] Delivered Successfully! (Status %d)", eventID, resp.StatusCode)
			db.Exec("UPDATE events SET status = 'completed' WHERE id = $1", eventID)
		}

		msg.Ack(false)
	}
}
