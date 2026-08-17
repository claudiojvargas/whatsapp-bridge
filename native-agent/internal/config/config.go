package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	AgentName    string
	PollInterval time.Duration
	LogLevel     string

	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	RedisEventsStream  string
	RedisCommandsQueue string
	WhatsAppDBPath     string
	StatePath          string
}

func Load() (Config, error) {
	pollIntervalRaw := getEnv(
		"AGENT_POLL_INTERVAL",
		"5s",
	)

	pollInterval, err := time.ParseDuration(pollIntervalRaw)

	if err != nil {
		return Config{}, fmt.Errorf(
			"invalid Agent_POLL_INTERVAL %q: %w",
			pollIntervalRaw,
			err,
		)
	}

	if pollInterval <= 0 {
		return Config{}, fmt.Errorf(
			"AGENT_POLL_INTERVAL must be greater than zero",
		)
	}

	redisDBRaw := getEnv(
		"REDIS_DB",
		"0",
	)

	redisDB, err := strconv.Atoi(redisDBRaw)
	if err != nil {
		return Config{}, fmt.Errorf(
			"invalid REDIS_DB %q: %w",
			redisDBRaw,
			err,
		)
	}

	return Config{
		AgentName:    getEnv("AGENT_NAME", "whatsapp-agent"),
		PollInterval: pollInterval,
		LogLevel:     getEnv("LOG_LEVEL", "info"),

		RedisAddr: getEnv(
			"REDIS_ADDR",
			"10.0.2.2:6379",
		),

		RedisPassword: getEnv(
			"REDIS_PASSWORD",
			"",
		),

		RedisDB: redisDB,

		RedisEventsStream: getEnv(
			"REDIS_EVENTS_STREAM",
			"agent:events",
		),

		RedisCommandsQueue: getEnv(
			"REDIS_COMMANDS_QUEUE",
			"android:commands",
		),

		WhatsAppDBPath: getEnv(
			"WHATSAPP_DB_PATH",
			"/data/user/0/com.whatsapp/databases/msgstore.db",
		),

		StatePath: getEnv(
			"AGENT_STATE_PATH",
			"/data/local/tmp/native-agent-state.json",
		),
	}, nil
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}
