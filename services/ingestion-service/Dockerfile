FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/ingestion-service .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/ingestion-service /ingestion-service

EXPOSE 8080
ENTRYPOINT ["/ingestion-service"]
