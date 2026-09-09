package alertmanager

// Transformer serializes one typed alert into one JSON request body.
type Transformer interface {
	Transform(Alert) ([]byte, error)
}

// TransformerFunc adapts a function into a Transformer.
type TransformerFunc func(Alert) ([]byte, error)

// Transform implements Transformer.
func (f TransformerFunc) Transform(alert Alert) ([]byte, error) {
	return f(alert)
}

// Decoder reconstructs one typed alert from one JSON request body.
type Decoder interface {
	Decode([]byte) (Alert, error)
}

// DecoderFunc adapts a function into a Decoder.
type DecoderFunc func([]byte) (Alert, error)

// Decode implements Decoder.
func (f DecoderFunc) Decode(body []byte) (Alert, error) {
	return f(body)
}
