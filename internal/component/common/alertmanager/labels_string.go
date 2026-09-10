package alertmanager

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// LabelsToString serializes a string map in sorted key order. Reserved bytes
// are percent-encoded so the result is also safe inside a JSON string literal.
func LabelsToString(labels map[string]string) (string, error) {
	keys := make([]string, 0, len(labels))
	for key, value := range labels {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return "", fmt.Errorf("labels must contain valid UTF-8")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var result strings.Builder
	result.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			result.WriteByte(',')
		}
		writeLabelToken(&result, key)
		result.WriteByte(':')
		writeLabelToken(&result, labels[key])
	}
	result.WriteByte('}')
	return result.String(), nil
}

func reservedLabelByte(b byte) bool {
	return b < 0x20 || b == 0x7f || strings.ContainsRune(`%,:{}\"`, rune(b))
}

func writeLabelToken(out *strings.Builder, value string) {
	const hex = "0123456789ABCDEF"
	for i := 0; i < len(value); i++ {
		b := value[i]
		if reservedLabelByte(b) {
			out.WriteByte('%')
			out.WriteByte(hex[b>>4])
			out.WriteByte(hex[b&15])
		} else {
			out.WriteByte(b)
		}
	}
}

// LabelsFromString reverses LabelsToString. Malformed input and duplicate keys
// return an error and no partial map. Label validity is checked by the caller.
func LabelsFromString(value string) (map[string]string, error) {
	if len(value) < 2 || value[0] != '{' || value[len(value)-1] != '}' {
		return nil, fmt.Errorf("serialized labels must be enclosed in braces")
	}
	result := make(map[string]string)
	if value == "{}" {
		return result, nil
	}
	for _, entry := range strings.Split(value[1:len(value)-1], ",") {
		key, raw, found := strings.Cut(entry, ":")
		if !found {
			return nil, fmt.Errorf("serialized label must contain a colon")
		}
		key, err := readLabelToken(key)
		if err != nil {
			return nil, err
		}
		decoded, err := readLabelToken(raw)
		if err != nil {
			return nil, err
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate serialized label %q", key)
		}
		result[key] = decoded
	}
	return result, nil
}

func readLabelToken(value string) (string, error) {
	var result strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b == '%' {
			if i+2 >= len(value) {
				return "", fmt.Errorf("incomplete serialized label escape")
			}
			hi, lo := hexDigit(value[i+1]), hexDigit(value[i+2])
			if hi < 0 || lo < 0 {
				return "", fmt.Errorf("invalid serialized label escape")
			}
			result.WriteByte(byte(hi<<4 | lo))
			i += 2
		} else if reservedLabelByte(b) {
			return "", fmt.Errorf("unescaped reserved byte in serialized label")
		} else {
			result.WriteByte(b)
		}
	}
	if !utf8.ValidString(result.String()) {
		return "", fmt.Errorf("serialized label must contain valid UTF-8")
	}
	return result.String(), nil
}

func hexDigit(b byte) int {
	switch {
	case b >= '0' && b <= '9':
		return int(b - '0')
	case b >= 'a' && b <= 'f':
		return int(b-'a') + 10
	case b >= 'A' && b <= 'F':
		return int(b-'A') + 10
	default:
		return -1
	}
}
