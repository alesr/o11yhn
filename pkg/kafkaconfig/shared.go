package kafkaconfig

import (
	"errors"
	"io/fs"
	"strings"
	"sync"

	"github.com/joho/godotenv"
)

var (
	loadEnvOnce sync.Once
	loadEnvErr  error
)

func loadDotEnv() error {
	loadEnvOnce.Do(func() {
		if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
			loadEnvErr = err
		}
	})
	return loadEnvErr
}

func normalizeCSV(values *[]string) {
	parts := *values
	filtered := make([]string, 0, len(parts))

	for _, value := range parts {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		filtered = append(filtered, trimmed)
	}
	*values = filtered
}
