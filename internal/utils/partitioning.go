package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

func CalculateShardingKey(appId string, numShards int) int {
	hash := sha256.Sum256([]byte(appId))
	hashInt := binary.BigEndian.Uint64(hash[:8])
	return int(hashInt%uint64(numShards)) + 1 // oo=jo con el +1. Hay que cambiarlo cuando escalemos el sistema. Modulo de algo con 1 siempre es 0.
}

func GetPartitioningKey(appId string, numPartitions int, prefix string) string {
	shardingKey := CalculateShardingKey(appId, numPartitions)
	routingKey := fmt.Sprintf("%v%d", prefix, shardingKey)
	return routingKey
}
