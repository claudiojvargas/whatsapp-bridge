package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type State struct {
	LastMessageID int64 `json:"last_message_id"`
}

type Store struct {
	path string
}

func New(path string) *Store {
	return &Store{
		path: path,
	}
}

func (s *Store) Load() (State, bool, error) {
	data, err := os.ReadFile(s.path)

	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}

	if err != nil {
		return State{}, false, fmt.Errorf(
			"read checkpoint: %w",
			err,
		)
	}

	var state State

	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, false, fmt.Errorf(
			"decode checkpoint: %w",
			err,
		)
	}

	return state, true, nil
}

func (s *Store) Save(state State) error {
	dir := filepath.Dir(s.path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf(
			"create checkpoint directory: %w",
			err,
		)
	}

	data, err := json.MarshalIndent(
		state,
		"",
		"  ",
	)

	if err != nil {
		return fmt.Errorf(
			"encode checkpoint: %w",
			err,
		)
	}

	tmpPath := s.path + ".tmp"

	if err := os.WriteFile(
		tmpPath,
		data,
		0644,
	); err != nil {
		return fmt.Errorf(
			"write checkpoint temp file: %w",
			err,
		)
	}

	if err := os.Rename(
		tmpPath,
		s.path,
	); err != nil {
		return fmt.Errorf(
			"replace checkpoint: %w",
			err,
		)
	}

	return nil
}
