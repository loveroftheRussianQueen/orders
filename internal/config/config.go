package config

import "os"

type Config struct {
	HTTPAddr    string
	DBURL       string
	KafkaBroker string
	RedisAddr   string
}

func Load() Config {
	return Config{
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		DBURL:       getEnv("DB_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable"),
		KafkaBroker: getEnv("KAFKA_BROKER", "localhost:9092"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
