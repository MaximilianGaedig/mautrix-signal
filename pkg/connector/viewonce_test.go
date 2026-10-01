package connector

import (
	"testing"

	"maunium.net/go/mautrix/event"
)

// The bridge framework rejects a media event carrying com.beeper.view_limited unless the room features list
// that exact limit for its message type, so a view-once photo only reaches the converter if this holds.
func TestViewOnceIsAdvertisedForPhotosAndVideosOnly(t *testing.T) {
	once := &event.BeeperViewLimitedMedia{Type: "count", Count: 1}
	for _, caps := range []*event.RoomFeatures{signalCaps, signalCapsDM, signalCapsNoteToSelf} {
		for msgType, feat := range caps.File {
			want := msgType == event.MsgImage || msgType == event.MsgVideo
			if got := feat.SupportsViewLimitedType(once); got != want {
				t.Errorf("%s: %s view-once = %v, want %v", caps.ID, msgType, got, want)
			}
		}
	}
}
