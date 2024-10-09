package utils

import (
	"errors"
	"os"
	"strconv"
)

func GetEnv(key string) (string, error) {
	value := os.Getenv(key)
	if value == "" {
		return "", errors.New("environment variable " + key + " not set")
	}
	return value, nil
}

func GetEnvInt(key string) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return 0, errors.New("environment variable " + key + " not set")
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New("environment variable " + key + " is not a valid integer")
	}

	return intValue, nil
}
