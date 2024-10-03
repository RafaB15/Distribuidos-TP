package utils

func ParseBoolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}
