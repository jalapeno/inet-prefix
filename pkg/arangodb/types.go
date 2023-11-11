package arangodb

type inetPrefix struct {
	Key       string `json:"_key,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
	PrefixLen int32  `json:"prefix_len,omitempty"`
	OriginAS  int32  `json:"origin_as"`
}
