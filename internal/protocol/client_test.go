package protocol

import (
	"strings"
	"testing"
)

func TestDecodeClient(t *testing.T) {
	edit := []byte(`{"type":"edit","request_id":"a-1","base_version":0,"op":{"pos":1,"delete_count":0,"text":"中😀"}}`)
	msg, err := DecodeClient(edit)
	if err != nil {
		t.Fatalf("decode edit: %v", err)
	}
	if msg.Type != "edit" || msg.RequestID != "a-1" || msg.BaseVersion != 0 || msg.Op != (Splice{1, 0, "中😀"}) {
		t.Fatalf("unexpected edit: %+v", msg)
	}
	msg, err = DecodeClient([]byte(`{"request_id":"s-1","type":"sync"}`))
	if err != nil || msg.Type != "sync" || msg.RequestID != "s-1" {
		t.Fatalf("decode sync: %+v, %v", msg, err)
	}
}

func TestDecodeClientRejectsInvalidMessages(t *testing.T) {
	cases := map[string][]byte{
		"forged user":     []byte(`{"type":"sync","request_id":"s","user_id":"alice"}`),
		"forged document": []byte(`{"type":"sync","request_id":"s","document_id":"doc"}`),
		"forged role":     []byte(`{"type":"sync","request_id":"s","role":"owner"}`),
		"nested identity": []byte(`{"type":"edit","request_id":"e","base_version":0,"op":{"pos":0,"delete_count":0,"text":"x","user_id":"bob"}}`),
		"sync edit field": []byte(`{"type":"sync","request_id":"s","base_version":0}`),
		"duplicate key":   []byte(`{"type":"sync","request_id":"s","request_id":"t"}`),
		"missing request": []byte(`{"type":"sync"}`),
		"empty request":   []byte(`{"type":"sync","request_id":"  "}`),
		"null request":    []byte(`{"type":"sync","request_id":null}`),
		"missing version": []byte(`{"type":"edit","request_id":"e","op":{"pos":0,"delete_count":0,"text":"x"}}`),
		"missing op":      []byte(`{"type":"edit","request_id":"e","base_version":0}`),
		"missing text":    []byte(`{"type":"edit","request_id":"e","base_version":0,"op":{"pos":0,"delete_count":1}}`),
		"null text":       []byte(`{"type":"edit","request_id":"e","base_version":0,"op":{"pos":0,"delete_count":1,"text":null}}`),
		"fraction":        []byte(`{"type":"edit","request_id":"e","base_version":1.0,"op":{"pos":0,"delete_count":0,"text":"x"}}`),
		"exponent":        []byte(`{"type":"edit","request_id":"e","base_version":1e0,"op":{"pos":0,"delete_count":0,"text":"x"}}`),
		"negative":        []byte(`{"type":"edit","request_id":"e","base_version":0,"op":{"pos":-1,"delete_count":0,"text":"x"}}`),
		"empty operation": []byte(`{"type":"edit","request_id":"e","base_version":0,"op":{"pos":0,"delete_count":0,"text":""}}`),
		"lone surrogate":  []byte(`{"type":"edit","request_id":"e","base_version":0,"op":{"pos":0,"delete_count":0,"text":"\ud800"}}`),
		"trailing data":   []byte(`{"type":"sync","request_id":"s"} true`),
		"array":           []byte(`[{"type":"sync","request_id":"s"}]`),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeClient(payload); err == nil {
				t.Fatalf("accepted invalid message: %s", payload)
			}
		})
	}
	invalidUTF8 := append([]byte(`{"type":"sync","request_id":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	if _, err := DecodeClient(invalidUTF8); err == nil {
		t.Fatal("accepted invalid UTF-8")
	}
	if _, err := DecodeClient([]byte(`{"type":"sync","request_id":"` + strings.Repeat("中", 43) + `"}`)); err == nil {
		t.Fatal("accepted request_id longer than 128 UTF-8 bytes")
	}
}

func TestPayloadHashUsesParsedValues(t *testing.T) {
	first, err := DecodeClient([]byte(`{"type":"edit","request_id":"a","base_version":3,"op":{"pos":1,"delete_count":0,"text":"<😀"}}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := DecodeClient([]byte(` { "op": { "text":"\u003c\ud83d\ude00", "delete_count":0,"pos":1},"base_version":3,"request_id":"b","type":"edit" } `))
	if err != nil {
		t.Fatal(err)
	}
	hash := PayloadHash(first.BaseVersion, first.Op)
	if hash != PayloadHash(second.BaseVersion, second.Op) || len(hash) != 64 {
		t.Fatalf("equivalent parsed operations have different hashes: %s / %s", hash, PayloadHash(second.BaseVersion, second.Op))
	}
	if hash == PayloadHash(4, first.Op) || hash == PayloadHash(3, Splice{1, 0, "other"}) {
		t.Fatal("different operation or version has same hash")
	}
	if strings.Contains(hash, "<") {
		t.Fatal("hash is not hex encoded")
	}
}
