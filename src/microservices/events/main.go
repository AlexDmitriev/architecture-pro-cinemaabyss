package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	movieWriter   *kafka.Writer
	userWriter    *kafka.Writer
	paymentWriter *kafka.Writer
	movieReader   *kafka.Reader
	userReader    *kafka.Reader
	paymentReader *kafka.Reader
)

func main() {
	// Инициализация Kafka
	initKafka()
	defer closeKafka()

	// Запуск consumers в отдельных горутинах
	go startMovieConsumer()
	go startUserConsumer()
	go startPaymentConsumer()

	// Регистрация маршрутов
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleMovie)
	http.HandleFunc("/api/events/user", handleUser)
	http.HandleFunc("/api/events/payment", handlePayment)

	// Получение порта из переменной окружения или использование значения по умолчанию
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("Starting events microservice on port %s", port)

	// Graceful shutdown
	go func() {
		log.Fatal(http.ListenAndServe(":"+port, nil))
	}()

	// Ожидание сигнала для graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")
}

func initKafka() {
	// Получение адреса Kafka брокера из переменной окружения
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	// Инициализация Kafka Producers
	movieWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers),
		Topic:    "movie-events",
		Balancer: &kafka.LeastBytes{},
	}

	userWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers),
		Topic:    "user-events",
		Balancer: &kafka.LeastBytes{},
	}

	paymentWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers),
		Topic:    "payment-events",
		Balancer: &kafka.LeastBytes{},
	}

	// Инициализация Kafka Consumers
	movieReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    "movie-events",
		GroupID:  "events-service-group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	userReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    "user-events",
		GroupID:  "events-service-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	paymentReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    "payment-events",
		GroupID:  "events-service-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Println("Kafka initialized successfully")
}

func closeKafka() {
	if movieWriter != nil {
		if err := movieWriter.Close(); err != nil {
			log.Printf("Error closing movie writer: %v", err)
		}
	}
	if userWriter != nil {
		if err := userWriter.Close(); err != nil {
			log.Printf("Error closing user writer: %v", err)
		}
	}
	if paymentWriter != nil {
		if err := paymentWriter.Close(); err != nil {
			log.Printf("Error closing payment writer: %v", err)
		}
	}
	if movieReader != nil {
		if err := movieReader.Close(); err != nil {
			log.Printf("Error closing movie reader: %v", err)
		}
	}
	if userReader != nil {
		if err := userReader.Close(); err != nil {
			log.Printf("Error closing user reader: %v", err)
		}
	}
	if paymentReader != nil {
		if err := paymentReader.Close(); err != nil {
			log.Printf("Error closing payment reader: %v", err)
		}
	}
}

func startMovieConsumer() {
	log.Println("Starting movie events consumer...")
	for {
		msg, err := movieReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading movie message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("Received movie event: Topic=%s Partition=%d Offset=%d Key=%s Value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		processMovieEvent(msg.Value)
	}
}

func startUserConsumer() {
	log.Println("Starting user events consumer...")
	for {
		msg, err := userReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading user message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("Received user event: Topic=%s Partition=%d Offset=%d Key=%s Value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		processUserEvent(msg.Value)
	}
}

func startPaymentConsumer() {
	log.Println("Starting payment events consumer...")
	for {
		msg, err := paymentReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading payment message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("Received payment event: Topic=%s Partition=%d Offset=%d Key=%s Value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		processPaymentEvent(msg.Value)
	}
}

func processMovieEvent(data []byte) {
	var event map[string]interface{}
	if err := json.Unmarshal(data, &event); err != nil {
		log.Printf("Error unmarshaling movie event: %v", err)
		return
	}

	log.Printf("Processing movie event: %+v", event)
	// Здесь можно добавить логику обработки события
}

func processUserEvent(data []byte) {
	var event map[string]interface{}
	if err := json.Unmarshal(data, &event); err != nil {
		log.Printf("Error unmarshaling user event: %v", err)
		return
	}

	log.Printf("Processing user event: %+v", event)
	// Здесь можно добавить логику обработки события
}

func processPaymentEvent(data []byte) {
	var event map[string]interface{}
	if err := json.Unmarshal(data, &event); err != nil {
		log.Printf("Error unmarshaling payment event: %v", err)
		return
	}

	log.Printf("Processing payment event: %+v", event)
	// Здесь можно добавить логику обработки события
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovie(w http.ResponseWriter, r *http.Request) {
	// Отправка сообщения в Kafka
	message := map[string]string{
		"event":     "Movie created",
		"timestamp": time.Now().Format(time.RFC3339),
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Отправка сообщения в Kafka
	err = movieWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("movie-event"),
			Value: messageJSON,
		},
	)

	if err != nil {
		log.Printf("Error writing message to Kafka: %v", err)
		http.Error(w, "Failed to send event", http.StatusInternalServerError)
		return
	}

	log.Printf("Movie event sent to Kafka: %s", string(messageJSON))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Movie event sent to Kafka",
	})
}

func handleUser(w http.ResponseWriter, r *http.Request) {
	// Отправка сообщения в Kafka
	message := map[string]string{
		"event":     "User created",
		"timestamp": time.Now().Format(time.RFC3339),
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Отправка сообщения в Kafka
	err = userWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("user-event"),
			Value: messageJSON,
		},
	)

	if err != nil {
		log.Printf("Error writing message to Kafka: %v", err)
		http.Error(w, "Failed to send event", http.StatusInternalServerError)
		return
	}

	log.Printf("User event sent to Kafka: %s", string(messageJSON))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "User event sent to Kafka",
	})
}

func handlePayment(w http.ResponseWriter, r *http.Request) {
	// Отправка сообщения в Kafka
	message := map[string]string{
		"event":     "Payment created",
		"timestamp": time.Now().Format(time.RFC3339),
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Отправка сообщения в Kafka
	err = paymentWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("payment-event"),
			Value: messageJSON,
		},
	)

	if err != nil {
		log.Printf("Error writing message to Kafka: %v", err)
		http.Error(w, "Failed to send event", http.StatusInternalServerError)
		return
	}

	log.Printf("Payment event sent to Kafka: %s", string(messageJSON))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Payment event sent to Kafka",
	})
}