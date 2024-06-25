package protocol

type errorResp struct {
	Msg     string              `json:"message"`
	Delay   int                 `json:"delay_seconds"`
	Headers map[string][]string `json:"headers"`
	Requeue bool                `json:"requeue"`
}
