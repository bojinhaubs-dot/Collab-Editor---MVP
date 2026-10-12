package auth

import (
	"errors"
	"net/http"
	"os"
	"strings"
)

// Tokens maps configured bearer secrets to demo user IDs. Roles still come
// exclusively from each document's server-side membership state.
type Tokens map[string]string

func FromEnv() (Tokens, error) {
	users := []string{"ADMIN", "ALICE", "BOB", "VIEWER"}
	tokens := make(Tokens, len(users))
	for _, name := range users {
		secret := os.Getenv("TOKEN_" + name)
		if secret == "" {
			return nil, errors.New("TOKEN_" + name + " is required")
		}
		if _, exists := tokens[secret]; exists {
			return nil, errors.New("demo bearer tokens must be distinct")
		}
		tokens[secret] = strings.ToLower(name)
	}
	return tokens, nil
}

func (t Tokens) User(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	secret := strings.TrimPrefix(header, "Bearer ")
	if strings.TrimSpace(secret) != secret || secret == "" {
		return "", false
	}
	user, ok := t[secret]
	return user, ok
}
