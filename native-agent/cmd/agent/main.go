package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"native-agent/internal/agent"
	"native-agent/internal/config"
	"native-agent/internal/logger"
	"native-agent/internal/native"
	"native-agent/internal/redisclient"

	"native-agent/internal/checkpoint"
	"native-agent/internal/contacts"
	"native-agent/internal/whatsapp"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to load config: %v\n",
			err,
		)

		os.Exit(1)
	}

	log, err := logger.New(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to initialize logger: %v\n",
			err,
		)

		os.Exit(1)
	}

	log.Info(
		"native environment initialized",
		"probe",
		native.Probe(),
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop()

	redisClient := redisclient.New(
		cfg.RedisAddr,
		cfg.RedisPassword,
		cfg.RedisDB,
		cfg.RedisEventsStream,
		cfg.RedisCommandsQueue,
	)

	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Error(
				"failed to close redis",
				"error",
				err,
			)
		}
	}()

	whatsappStore, err := whatsapp.Open(
		cfg.WhatsAppDBPath,
	)

	if err != nil {
		log.Error(
			"failed to open whatsapp database",
			"error",
			err,
		)

		os.Exit(1)
	}

	defer func() {
		if err := whatsappStore.Close(); err != nil {
			log.Error(
				"failed to close whatsapp database",
				"error",
				err,
			)
		}
	}()

	checkpointStore := checkpoint.New(
		cfg.StatePath,
	)

	contactWriter := contacts.New()

	app := agent.New(
		cfg,
		log,
		redisClient,
		whatsappStore,
		checkpointStore,
		contactWriter,
	)

	if err := app.Run(ctx); err != nil {
		log.Error(
			"agent stopped with error",
			"error",
			err,
		)

		os.Exit(1)
	}

	log.Info("agent stopped")
}
