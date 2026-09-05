package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type WebhookEvent struct {
	ID        string    `json:"id"`
	TargetURL string    `json:"target_url"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB
var mqConn *amqp.Connection
var mqChannel *amqp.Channel

func initDB() {
	var err error
	connStr := "postgres://hookuser:hookpass@localhost:5432/hookforge?sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to open DB:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("Database is unreachable:", err)
	}
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS events (
		id VARCHAR(50) PRIMARY KEY,
		target_url TEXT NOT NULL,
		payload JSONB NOT NULL,
		status VARCHAR(20) DEFAULT 'pending',
		created_at TIMESTAMP
		);`
		_, err = db.Exec(createTableQuery)
		if err != nil {
			log.Fatal("Failed to create table:", err)
		}
		log.Println("Database connected.")
}

func initRabbitMQ() {
	var err error
	// Connect to RabbitMQ via the default port 5672 with default guest/guest credentials
	mqConn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}

	// Open a channel (a multiplexed connection over the TCP socket)
	mqChannel, err = mqConn.Channel()
	if err != nil {
		log.Fatal("Failed to open a channel:", err)
	}

	// Declare our queue. If it doesn't exist, RabbitMQ creates it.
	_, err = mqChannel.QueueDeclare(
		"webhook_jobs", // queue name
		true,           // durable (survives RabbitMQ restarts)
	false,          // auto-delete when unused
	false,          // exclusive
	false,          // no-wait
	nil,            // arguments
	)
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}
	log.Println("RabbitMQ connected and 'webhook_jobs' queue verified.")
}

func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event WebhookEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	event.ID = fmt.Sprintf("evt_%d", time.Now().UnixNano())
	event.CreatedAt = time.Now()

	// 1. Durably save to PostgreSQL
	insertQuery := `INSERT INTO events (id, target_url, payload, created_at) VALUES ($1, $2, $3, $4)`
	_, err := db.Exec(insertQuery, event.ID, event.TargetURL, event.Payload, event.CreatedAt)
	if err != nil {
		log.Printf("DB Error: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	log.Printf("[SAVED TO DB] ID: %s", event.ID)

	// 2. Publish the Event ID to RabbitMQ
	err = mqChannel.PublishWithContext(
		r.Context(),
					   "",             // default exchange
					   "webhook_jobs", // routing key (queue name)
	false,          // mandatory
	false,          // immediate
	amqp.Publishing{
		DeliveryMode: amqp.Persistent,     // ensure message is written to disk
		ContentType:  "text/plain",
		Body:         []byte(event.ID),    // We only send the ID!
	},
	)
	if err != nil {
		log.Printf("MQ Publish Error: %v", err)
		// Note: In a production system, we'd handle this via a DB sweeper (Outbox Pattern)
	} else {
		log.Printf("[PUBLISHED TO MQ] ID: %s", event.ID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "accepted",
		"event_id": event.ID,
	})
}

func main() {
	initDB()
	initRabbitMQ()

	// Ensure connections close gracefully when the program exits
	defer db.Close()
	defer mqChannel.Close()
	defer mqConn.Close()

	http.HandleFunc("/api/v1/webhooks", handleIngest)
	log.Println("HookForge API running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
