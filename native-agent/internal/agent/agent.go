package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"native-agent/internal/checkpoint"
	"native-agent/internal/config"
	"native-agent/internal/whatsapp"
)

type Command struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Number string `json:"number"`
}

type RedisClient interface {
	Ping(ctx context.Context) error

	PublishEvent(
		ctx context.Context,
		eventType string,
		fields map[string]any,
	) (string, error)

	WaitCommand(
		ctx context.Context,
		timeout time.Duration,
	) (string, error)
}

type WhatsAppStore interface {
	Ping(ctx context.Context) error

	LatestMessageID(
		ctx context.Context,
	) (int64, error)

	MessagesAfter(
		ctx context.Context,
		lastID int64,
		limit int,
	) ([]whatsapp.Message, error)
}

type CheckpointStore interface {
	Load() (checkpoint.State, bool, error)
	Save(state checkpoint.State) error
}

type ContactWriter interface {
	Create(
		ctx context.Context,
		name string,
		number string,
	) (int64, error)
}

type Agent struct {
	config config.Config
	logger *slog.Logger
	redis  RedisClient

	whatsapp WhatsAppStore
	state    CheckpointStore
	contacts ContactWriter

	lastMessageID int64
}

func New(
	cfg config.Config,
	logger *slog.Logger,
	redis RedisClient,
	whatsapp WhatsAppStore,
	state CheckpointStore,
	contacts ContactWriter,
) *Agent {
	return &Agent{
		config:   cfg,
		logger:   logger,
		redis:    redis,
		whatsapp: whatsapp,
		state:    state,
		contacts: contacts,
	}
}

func (a *Agent) Run(ctx context.Context) error {
	a.logger.Info(
		"connecting to redis",
		"addr",
		a.config.RedisAddr,
	)

	if err := a.redis.Ping(ctx); err != nil {
		return err
	}

	a.logger.Info(
		"redis connected",
		"addr",
		a.config.RedisAddr,
	)

	a.logger.Info(
		"connecting to whatsapp database",
		"path",
		a.config.WhatsAppDBPath,
	)

	if err := a.whatsapp.Ping(ctx); err != nil {
		return err
	}

	savedState, exists, err := a.state.Load()
	if err != nil {
		return err
	}

	if exists {
		a.lastMessageID = savedState.LastMessageID

		a.logger.Info(
			"checkpoint loaded",
			"last_message_id",
			a.lastMessageID,
		)

	} else {
		latestMessageID, err := a.whatsapp.LatestMessageID(ctx)
		if err != nil {
			return err
		}

		a.lastMessageID = latestMessageID

		if err := a.state.Save(
			checkpoint.State{
				LastMessageID: a.lastMessageID,
			},
		); err != nil {
			return err
		}

		a.logger.Info(
			"checkpoint initialized",
			"last_message_id",
			a.lastMessageID,
		)
	}

	eventID, err := a.redis.PublishEvent(
		ctx,
		"agent.started",
		map[string]any{
			"agent_name": a.config.AgentName,
		},
	)

	if err != nil {
		return err
	}

	a.logger.Info(
		"startup event published",
		"stream",
		a.config.RedisEventsStream,
		"event_id",
		eventID,
	)

	a.logger.Info(
		"agent started",
		"name",
		a.config.AgentName,
		"poll_interval",
		a.config.PollInterval,
	)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		a.consumeCommands(ctx)
	}()

	defer wg.Wait()

	ticker := time.NewTicker(
		a.config.PollInterval,
	)

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.logger.Info(
				"shutdown signal received",
				"name",
				a.config.AgentName,
			)

			return nil

		case now := <-ticker.C:
			a.tick(ctx, now)
		}
	}
}

func (a *Agent) tick(
	ctx context.Context,
	now time.Time,
) {
	if ctx.Err() != nil {
		return
	}

	a.logger.Debug(
		"checking whatsapp messages",
		"last_message_id",
		a.lastMessageID,
		"time",
		now.Format(time.RFC3339),
	)

	messages, err := a.whatsapp.MessagesAfter(
		ctx,
		a.lastMessageID,
		100,
	)

	if err != nil {
		a.logger.Error(
			"failed reading whatsapp messages",
			"error",
			err,
		)

		return
	}

	for _, message := range messages {
		eventID, err := a.redis.PublishEvent(
			ctx,
			"whatsapp.message",
			map[string]any{
				"message_id":        message.ID,
				"chat_row_id":       message.ChatRowID,
				"sender_jid_row_id": message.SenderJIDRowID,

				"chat_jid":     message.ChatJID,
				"sender_jid":   message.SenderJID,
				"chat_subject": message.ChatSubject,

				"from_me":      message.FromMe,
				"timestamp":    message.Timestamp,
				"message_type": message.MessageType,
				"text":         message.Text,
			},
		)

		if err != nil {
			a.logger.Error(
				"failed publishing whatsapp message",
				"message_id",
				message.ID,
				"error",
				err,
			)

			return
		}

		a.lastMessageID = message.ID

		if err := a.state.Save(
			checkpoint.State{
				LastMessageID: a.lastMessageID,
			},
		); err != nil {
			a.logger.Error(
				"failed saving checkpoint",
				"message_id",
				message.ID,
				"error",
				err,
			)

			return
		}

		a.logger.Info(
			"whatsapp message published",
			"message_id",
			message.ID,
			"event_id",
			eventID,
			"from_me",
			message.FromMe,
		)
	}
}

func (a *Agent) consumeCommands(
	ctx context.Context,
) {
	a.logger.Info(
		"redis command consumer started",
		"queue",
		a.config.RedisCommandsQueue,
	)

	for {
		if ctx.Err() != nil {
			return
		}

		command, err := a.redis.WaitCommand(
			ctx,
			2*time.Second,
		)

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			a.logger.Error(
				"failed waiting for redis command",
				"error",
				err,
			)

			select {
			case <-ctx.Done():
				return

			case <-time.After(time.Second):
			}

			continue
		}

		if command == "" {
			continue
		}

		a.logger.Info(
			"redis command received",
			"payload",
			command,
		)

		var cmd Command

		if err := json.Unmarshal(
			[]byte(command),
			&cmd,
		); err != nil {
			a.logger.Error(
				"invalid redis command",
				"error",
				err,
				"payload",
				command,
			)

			continue
		}

		switch cmd.Type {
		case "contact.create":
			name := strings.TrimSpace(cmd.Name)
			number := strings.TrimSpace(cmd.Number)

			if name == "" || number == "" {
				a.logger.Error(
					"invalid contact command",
					"name",
					name,
					"number",
					number,
				)

				continue
			}

			rawContactID, err := a.contacts.Create(
				ctx,
				name,
				number,
			)

			if err != nil {
				a.logger.Error(
					"failed creating android contact",
					"name",
					name,
					"number",
					number,
					"error",
					err,
				)

				_, publishErr := a.redis.PublishEvent(
					ctx,
					"contact.failed",
					map[string]any{
						"name":   name,
						"number": number,
						"error":  err.Error(),
					},
				)

				if publishErr != nil {
					a.logger.Error(
						"failed publishing contact failure",
						"error",
						publishErr,
					)
				}

				continue
			}

			eventID, err := a.redis.PublishEvent(
				ctx,
				"contact.created",
				map[string]any{
					"name":           name,
					"number":         number,
					"raw_contact_id": rawContactID,
				},
			)

			if err != nil {
				a.logger.Error(
					"failed publishing contact created event",
					"error",
					err,
				)
			}

			a.logger.Info(
				"android contact created",
				"name",
				name,
				"number",
				number,
				"raw_contact_id",
				rawContactID,
				"event_id",
				eventID,
			)

		default:
			a.logger.Warn(
				"unsupported redis command",
				"type",
				cmd.Type,
			)
		}

	}
}
