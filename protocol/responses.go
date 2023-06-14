package protocol

type errorResp struct {
	Msg     string              `json:"message"`
	Delay   int64               `json:"delay_seconds"`
	Headers map[string][]string `json:"headers"`
	Requeue bool                `json:"requeue"`
}
