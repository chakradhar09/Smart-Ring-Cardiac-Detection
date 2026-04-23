package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type BiosignalPayload struct {
	ReadingID  string                 `json:"reading_id,omitempty"`
	UserID     string                 `json:"user_id"`
	DeviceID   string                 `json:"device_id"`
	ECGValue   float64                `json:"ecg_value"`
	PPGValue   float64                `json:"ppg_value"`
	HeartRate  float64                `json:"heart_rate"`
	HRV        float64                `json:"hrv"`
	SpO2       float64                `json:"spo2"`
	Location   map[string]float64     `json:"location,omitempty"`
	RecordedAt string                 `json:"recorded_at,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type IngestionResponse struct {
	Status    string `json:"status"`
	ReadingID string `json:"reading_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

func main() {
	port := getEnv("PORT", "8080")
	topic := getEnv("KAFKA_TOPIC_RAW", "biosignal.raw")
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		RequiredAcks: int(kafka.RequireAll),
		Balancer:     &kafka.LeastBytes{},
	})
	defer writer.Close()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, IngestionResponse{Status: "ok"})
	})

	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, IngestionResponse{Status: "error", Error: "only POST is supported"})
			return
		}

		var payload BiosignalPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, IngestionResponse{Status: "error", Error: "invalid JSON payload"})
			return
		}

		if payload.UserID == "" || payload.DeviceID == "" {
			writeJSON(w, http.StatusBadRequest, IngestionResponse{Status: "error", Error: "user_id and device_id are required"})
			return
		}

		if payload.ReadingID == "" {
			payload.ReadingID = uuid.NewString()
		}
		if payload.RecordedAt == "" {
			payload.RecordedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}

		message, err := json.Marshal(payload)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, IngestionResponse{Status: "error", Error: "failed to serialize payload"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(payload.UserID),
			Value: message,
			Time:  time.Now().UTC(),
		})
		if err != nil {
			log.Printf("failed to publish reading %s: %v", payload.ReadingID, err)
			writeJSON(w, http.StatusBadGateway, IngestionResponse{Status: "error", Error: "unable to publish to stream"})
			return
		}

		log.Printf("published reading=%s user=%s device=%s", payload.ReadingID, payload.UserID, payload.DeviceID)
		writeJSON(w, http.StatusAccepted, IngestionResponse{Status: "accepted", ReadingID: payload.ReadingID})
	})

	log.Printf("ingestion service listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, body IngestionResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
