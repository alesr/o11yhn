package watermark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/renameio"
)

const defaultStatePath = ".state/producer-watermark.json"

type Store interface {
	Load(ctx context.Context) (int64, error)
	Save(ctx context.Context, lastTime int64) error
}

type FileStore struct{ path string }

type state struct {
	LastTime int64 `json:"last_time"`
}

func NewFileStore(path string) *FileStore {
	if path == "" {
		path = defaultStatePath
	}
	return &FileStore{path: path}
}

func (s *FileStore) Load(_ context.Context) (int64, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return 0, err
	}

	var st state
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, fmt.Errorf("could not decode watermark state: %w", err)
	}
	return st.LastTime, nil
}

func (s *FileStore) Save(_ context.Context, lastTime int64) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("could not create watermark directory: %w", err)
	}

	st := state{LastTime: lastTime}

	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("could not encode watermark state: %w", err)
	}

	if err := renameio.WriteFile(s.path, data, 0o644); err != nil {
		return fmt.Errorf("could not persist watermark state: %w", err)
	}
	return nil
}
