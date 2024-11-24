package utils

import "hash/fnv"

func Hash(data []byte) (int, error) {
	hash64 := fnv.New64a()
	_, err := hash64.Write(data)
	if err != nil {
		return 0, err
	}
	return int(hash64.Sum64()), nil
}
