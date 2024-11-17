package persistence

import "encoding/binary"

type IntMap[T any] struct {
	contents    map[int]T
	serialize   func(T) []byte
	deserialize func([]byte) (T, error)
}

func NewIntMap[T any](serialize func(T) []byte, deserialize func([]byte) (T, error)) *IntMap[T] {
	return &IntMap[T]{contents: make(map[int]T)}
}

func (m *IntMap[T]) Get(key int) (T, bool) {
	value, exists := m.contents[key]
	return value, exists
}

func (m *IntMap[T]) Set(key int, value T) {
	m.contents[key] = value
}

func (m *IntMap[T]) Delete(key int) {
	delete(m.contents, key)
}

func (m *IntMap[T]) Serialize(n *IntMap[T]) []byte {
	serialized := make(map[int][]byte)
	for key, value := range n.contents {
		serialized[key] = n.serialize(value)
	}

	// Agregamos la cantidad total de entrada
	keyAmount := len(serialized)
	serializedMap := make([]byte, 4)
	binary.BigEndian.PutUint32(serializedMap, uint32(keyAmount))

	// Agregamos las entradas y su longitud
	for key, value := range serialized {
		// Agregamos la clave
		serializedKey := make([]byte, 8)
		binary.BigEndian.PutUint64(serializedKey, uint64(key))
		serializedMap = append(serializedMap, serializedKey...)

		// Agregamos la longitud de la entrada
		entrySize := len(value)
		entrySizeBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(entrySizeBytes, uint64(entrySize))
		serializedMap = append(serializedMap, entrySizeBytes...)

		// Agregamos la entrada
		serializedMap = append(serializedMap, value...)
	}

	return serializedMap
}

func (m *IntMap[T]) Deserialize(serializedMap []byte) (*IntMap[T], error) {
	newClientMap := NewIntMap(m.serialize, m.deserialize)

	// Obtenemos la cantidad total de entradas
	keyAmount := binary.BigEndian.Uint32(serializedMap[:4])
	offset := 4

	// Deserializamos las entradas
	for i := 0; i < int(keyAmount); i++ {
		// Obtenemos la clave
		key := int(binary.BigEndian.Uint64(serializedMap[offset : offset+8]))

		offset += 8

		// Obtenemos la longitud de la entrada
		entrySize := int(binary.BigEndian.Uint64(serializedMap[offset : offset+8]))
		offset += 8

		// Obtenemos la entrada
		value, err := newClientMap.deserialize(serializedMap[offset : offset+entrySize])
		if err != nil {
			return nil, err
		}
		newClientMap.Set(key, value)
		offset += entrySize
	}

	return newClientMap, nil
}
