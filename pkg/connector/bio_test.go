package connector

import (
	"encoding/json"
	"testing"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

func TestBioExtraProfile(t *testing.T) {
	extra := bioExtraProfile(types.Profile{About: "  Speak freely ", AboutEmoji: "🌱"}, false)
	raw, ok := extra[bioProfileKey]
	if !ok || len(extra) != 1 {
		t.Fatalf("expected only %s, got %v", bioProfileKey, extra)
	}
	var got struct {
		Text []struct {
			Body string `json:"body"`
		} `json:"m.text"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Text) != 1 || got.Text[0].Body != "🌱 Speak freely" {
		t.Fatalf("unexpected bio %s", raw)
	}
}

func TestBioTextVariants(t *testing.T) {
	for name, tc := range map[string]struct {
		in   types.Profile
		want string
	}{
		"about only": {types.Profile{About: "hi"}, "hi"},
		"emoji only": {types.Profile{AboutEmoji: "🌱"}, "🌱"},
		"both":       {types.Profile{About: "hi", AboutEmoji: "🌱"}, "🌱 hi"},
		"none":       {types.Profile{}, ""},
	} {
		if got := bioText(tc.in); got != tc.want {
			t.Errorf("%s: got %q, want %q", name, got, tc.want)
		}
	}
}

func TestRemovedBioIsClearedOnlyWhenThereWasOne(t *testing.T) {
	if got := bioExtraProfile(types.Profile{}, false); len(got) != 0 {
		t.Fatalf("no bio and none before must add nothing, got %v", got)
	}
	got := bioExtraProfile(types.Profile{}, true)
	if string(got[bioProfileKey]) != "null" {
		t.Fatalf("a removed bio must be cleared with null, got %v", got)
	}
}
