// Package protocol defines the client messages accepted over a document WebSocket.
package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Splice uses Unicode code point positions. The Room validates its range against
// the current document and enforces the resulting document size limit.
type Splice struct {
	Pos         int64  `json:"pos"`
	DeleteCount int64  `json:"delete_count"`
	Text        string `json:"text"`
}

type ClientMessage struct {
	Type        string `json:"type"`
	RequestID   string `json:"request_id"`
	BaseVersion int64  `json:"base_version,omitempty"`
	Op          Splice `json:"op,omitempty"`
}

// DecodeClient accepts exactly the fields defined by the edit and sync wire
// messages. In particular, identity and document routing cannot be supplied by
// a client message. A caller still has to check the WebSocket message size.
func DecodeClient(data []byte) (ClientMessage, error) {
	var msg ClientMessage
	if !utf8.Valid(data) || !validUnicodeEscapes(data) {
		return msg, errors.New("invalid UTF-8 or Unicode escape")
	}
	fields, err := decodeObject(data, "type", "request_id", "base_version", "op")
	if err != nil {
		return msg, err
	}
	if msg.Type, err = requiredString(fields, "type"); err != nil {
		return ClientMessage{}, err
	}
	if msg.RequestID, err = requiredString(fields, "request_id"); err != nil {
		return ClientMessage{}, err
	}
	if strings.TrimSpace(msg.RequestID) == "" {
		return ClientMessage{}, errors.New("request_id must not be empty")
	}
	if len(msg.RequestID) > 128 {
		return ClientMessage{}, errors.New("request_id exceeds 128 UTF-8 bytes")
	}

	switch msg.Type {
	case "sync":
		if len(fields) != 2 {
			return ClientMessage{}, errors.New("sync accepts only type and request_id")
		}
	case "edit":
		if msg.BaseVersion, err = requiredNonNegativeInt(fields, "base_version"); err != nil {
			return ClientMessage{}, err
		}
		opRaw, ok := fields["op"]
		if !ok {
			return ClientMessage{}, errors.New("missing op")
		}
		opFields, err := decodeObject(opRaw, "pos", "delete_count", "text")
		if err != nil {
			return ClientMessage{}, fmt.Errorf("op: %w", err)
		}
		if msg.Op.Pos, err = requiredNonNegativeInt(opFields, "pos"); err != nil {
			return ClientMessage{}, fmt.Errorf("op: %w", err)
		}
		if msg.Op.DeleteCount, err = requiredNonNegativeInt(opFields, "delete_count"); err != nil {
			return ClientMessage{}, fmt.Errorf("op: %w", err)
		}
		if msg.Op.Text, err = requiredString(opFields, "text"); err != nil {
			return ClientMessage{}, fmt.Errorf("op: %w", err)
		}
		if msg.Op.DeleteCount == 0 && msg.Op.Text == "" {
			return ClientMessage{}, errors.New("empty edit operation")
		}
	default:
		return ClientMessage{}, errors.New("unsupported message type")
	}
	return msg, nil
}

// PayloadHash hashes parsed values in a fixed field order, independent of JSON
// whitespace, key order, or escape spelling in the incoming message.
func PayloadHash(baseVersion int64, op Splice) string {
	payload := struct {
		BaseVersion int64  `json:"base_version"`
		Pos         int64  `json:"pos"`
		DeleteCount int64  `json:"delete_count"`
		Text        string `json:"text"`
	}{baseVersion, op.Pos, op.DeleteCount, op.Text}
	encoded, _ := json.Marshal(payload) // These concrete fields cannot fail to marshal.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func decodeObject(data []byte, allowed ...string) (map[string]json.RawMessage, error) {
	allowedKeys := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		allowedKeys[key] = true
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, errors.New("expected JSON object")
	}
	fields := make(map[string]json.RawMessage, len(allowed))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid object key: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("invalid object key")
		}
		if !allowedKeys[key] {
			return nil, fmt.Errorf("unknown field %q", key)
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", key, err)
		}
		fields[key] = raw
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return nil, errors.New("invalid JSON object ending")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing data after JSON object")
	}
	return fields, nil
}

func requiredString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("missing %s", name)
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '"' {
		return "", fmt.Errorf("%s must be a string", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("invalid %s: %w", name, err)
	}
	return value, nil
}

func requiredNonNegativeInt(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("missing %s", name)
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

// encoding/json replaces unpaired UTF-16 surrogate escapes with U+FFFD.
// Reject them instead so different malformed payloads cannot normalize to
// the same operation or document text.
func validUnicodeEscapes(data []byte) bool {
	inString := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || i+1 >= len(data) {
				continue
			}
			i++
			if data[i] != 'u' {
				continue
			}
			code, ok := hex4(data, i+1)
			if !ok {
				return false
			}
			i += 4
			if code >= 0xdc00 && code <= 0xdfff {
				return false
			}
			if code >= 0xd800 && code <= 0xdbff {
				if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
					return false
				}
				low, ok := hex4(data, i+3)
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			}
		}
	}
	return true
}

func hex4(data []byte, start int) (uint16, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	var value uint16
	for _, c := range data[start : start+4] {
		value <<= 4
		switch {
		case c >= '0' && c <= '9':
			value |= uint16(c - '0')
		case c >= 'a' && c <= 'f':
			value |= uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			value |= uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
