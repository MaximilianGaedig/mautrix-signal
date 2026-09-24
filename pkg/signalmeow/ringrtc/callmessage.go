package ringrtc

import "go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"

type MessageType string

const (
	MessageOffer  MessageType = "offer"
	MessageAnswer MessageType = "answer"
	MessageICE    MessageType = "ice_update"
	MessageBusy   MessageType = "busy"
	MessageHangup MessageType = "hangup"
	MessageOpaque MessageType = "opaque"
)

type CallType string

const (
	CallTypeAudio CallType = "audio"
	CallTypeVideo CallType = "video"
)

type CallMessage struct {
	ID                  uint64
	MessageType         MessageType
	Type                CallType
	DestinationDeviceID uint32
	HangupType          signalpb.CallMessage_Hangup_Type
	HangupDeviceID      uint32
	OpaqueUrgency       signalpb.CallMessage_Opaque_Urgency
	OpaqueLength        int
	Offer               *Offer
	Answer              *Answer
	ICECandidate        *IceCandidate
	ParseError          string
}

// DecodeCallMessage splits a Signal CallMessage into its RingRTC sub-messages.
// A CallMessage can contain multiple ICE updates, so each gets its own result.
func DecodeCallMessage(message *signalpb.CallMessage) []CallMessage {
	base := CallMessage{DestinationDeviceID: message.GetDestinationDeviceId()}
	var out []CallMessage
	if offer := message.GetOffer(); offer != nil {
		decoded := base
		decoded.ID = offer.GetId()
		decoded.MessageType = MessageOffer
		if offer.GetType() == signalpb.CallMessage_Offer_OFFER_VIDEO_CALL {
			decoded.Type = CallTypeVideo
		} else {
			decoded.Type = CallTypeAudio
		}
		var err error
		decoded.Offer, err = DecodeOffer(offer.GetOpaque())
		if err != nil {
			decoded.ParseError = err.Error()
		}
		out = append(out, decoded)
	}
	if answer := message.GetAnswer(); answer != nil {
		decoded := base
		decoded.ID = answer.GetId()
		decoded.MessageType = MessageAnswer
		var err error
		decoded.Answer, err = DecodeAnswer(answer.GetOpaque())
		if err != nil {
			decoded.ParseError = err.Error()
		}
		out = append(out, decoded)
	}
	for _, ice := range message.GetIceUpdate() {
		decoded := base
		decoded.ID = ice.GetId()
		decoded.MessageType = MessageICE
		var err error
		decoded.ICECandidate, err = DecodeIceCandidate(ice.GetOpaque())
		if err != nil {
			decoded.ParseError = err.Error()
		}
		out = append(out, decoded)
	}
	if busy := message.GetBusy(); busy != nil {
		decoded := base
		decoded.ID = busy.GetId()
		decoded.MessageType = MessageBusy
		out = append(out, decoded)
	}
	if hangup := message.GetHangup(); hangup != nil {
		decoded := base
		decoded.ID = hangup.GetId()
		decoded.MessageType = MessageHangup
		decoded.HangupType = hangup.GetType()
		decoded.HangupDeviceID = hangup.GetDeviceId()
		out = append(out, decoded)
	}
	if opaque := message.GetOpaque(); opaque != nil {
		decoded := base
		decoded.MessageType = MessageOpaque
		decoded.OpaqueUrgency = opaque.GetUrgency()
		decoded.OpaqueLength = len(opaque.GetData())
		out = append(out, decoded)
	}
	return out
}
