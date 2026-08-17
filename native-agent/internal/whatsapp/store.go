package whatsapp

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

type Message struct {
	ID             int64
	ChatRowID      int64
	FromMe         bool
	SenderJIDRowID int64
	Timestamp      int64
	MessageType    int
	Text           string

	ChatJID     string
	SenderJID   string
	ChatSubject string
}

func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf(
		"file:%s?mode=ro&_busy_timeout=5000&_query_only=1",
		path,
	)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf(
			"open whatsapp database: %w",
			err,
		)
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Ping(
	ctx context.Context,
) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf(
			"ping whatsapp database: %w",
			err,
		)
	}

	return nil
}

func (s *Store) LatestMessageID(
	ctx context.Context,
) (int64, error) {
	const query = `
		SELECT COALESCE(MAX(_id), 0)
		FROM message
	`

	var id int64

	if err := s.db.QueryRowContext(
		ctx,
		query,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf(
			"get latest whatsapp message id: %w",
			err,
		)
	}

	return id, nil
}

func (s *Store) MessagesAfter(
	ctx context.Context,
	lastID int64,
	limit int,
) ([]Message, error) {
	const query = `
	SELECT
		m._id,
		m.chat_row_id,
		m.from_me,
		COALESCE(m.sender_jid_row_id, 0),
		COALESCE(m.timestamp, 0),
		COALESCE(m.message_type, -1),
		COALESCE(m.text_data, ''),

		COALESCE(
			NULLIF(cj.raw_string, ''),
			cj.user || '@' || cj.server,
			''
		),

		COALESCE(
			NULLIF(sj.raw_string, ''),
			sj.user || '@' || sj.server,
			''
		),

		COALESCE(c.subject, '')

		FROM message m

		JOIN chat c
			ON c._id = m.chat_row_id

		JOIN jid cj
			ON cj._id = c.jid_row_id

		LEFT JOIN jid sj
			ON sj._id = m.sender_jid_row_id

		WHERE m._id > ?

		ORDER BY m._id ASC

		LIMIT ?
	`

	rows, err := s.db.QueryContext(
		ctx,
		query,
		lastID,
		limit,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"query whatsapp messages: %w",
			err,
		)
	}

	defer rows.Close()

	var messages []Message

	for rows.Next() {
		var message Message
		var fromMe int

		if err := rows.Scan(
			&message.ID,
			&message.ChatRowID,
			&fromMe,
			&message.SenderJIDRowID,
			&message.Timestamp,
			&message.MessageType,
			&message.Text,
			&message.ChatJID,
			&message.SenderJID,
			&message.ChatSubject,
		); err != nil {
			return nil, fmt.Errorf(
				"scan whatsapp message: %w",
				err,
			)
		}

		message.FromMe = fromMe != 0

		messages = append(
			messages,
			message,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate whatsapp messages: %w",
			err,
		)
	}

	return messages, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}
