package utils

import (
	"encoding/binary"
	"errors"
)

func SerializeBool(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func DeserializeBool(data []byte) (bool, error) {
	if len(data) != 1 {
		return false, errors.New("data too short to deserialize into bool")
	}
	if data[0] == 1 {
		return true, nil
	}
	return false, nil
}

func SerializeInt(i int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func DeserializeInt(data []byte) (int, error) {
	if len(data) != 8 {
		return 0, errors.New("data too short to deserialize into int")
	}
	return int(binary.BigEndian.Uint64(data)), nil
}
