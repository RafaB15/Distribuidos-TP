package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
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

func GetPartitioningKeyFromInt(appId int, numPartitions int, prefix string) string {
	appIdStr := fmt.Sprintf("%d", appId)
	return GetPartitioningKey(appIdStr, numPartitions, prefix)
}

func GetRandomNumber(max int) int {

	// Crear un nuevo generador de números aleatorios con una semilla basada en el tiempo
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// rand.Intn devuelve un número entre 0 y max-min, así que sumamos min para ajustarlo al rango
	return r.Intn(max+1-1) + 1
}
