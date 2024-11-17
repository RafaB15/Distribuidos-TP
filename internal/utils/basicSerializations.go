package utils

import "errors"

func SerializeBool(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func DeserializeBool(data []byte) (bool, error) {
	if len(data) != 1 {
		return false, errors.New("Data too short to deserialize into bool")
	}
	if data[0] == 1 {
		return true, nil
	}
	return false, nil
}
