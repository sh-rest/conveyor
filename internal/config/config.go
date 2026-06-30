package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port            string
	DatabaseURL     string
	RedisURL        string
	WorkerCount     int
	MaxRetries      int
	APIKeyPrefix    string
}

func Load() Config {
	return Config{
		Port:        getEnv("PORT", "8080"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://conveyor:conveyor@localhost:5432/conveyor?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
		WorkerCount: getEnvInt("WORKER_CONCURRENCY", 10),
		MaxRetries:  getEnvInt("MAX_RETRIES", 7),
		APIKeyPrefix: getEnv("API_KEY_PREFIX", "whk_live_"),
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}
