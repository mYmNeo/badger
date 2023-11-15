package badger

import "github.com/klauspost/compress/s2"

type Encoder func(dst, src []byte) []byte
type Decoder func(dst, src []byte) ([]byte, error)

func NewS2Encoder(level int) Encoder {
	switch level {
	case 1:
		return s2.EncodeBetter
	case 2:
		return s2.EncodeBest
	default:
	}

	return s2.Encode
}

func NewS2Decoder() Decoder {
	return s2.Decode
}
