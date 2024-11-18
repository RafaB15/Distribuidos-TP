package node

import (
	u "distribuidos-tp/internal/utils"
)

type EndOfFileController struct {
	remainingEOFsMap *IntMap[int]
	expectedEOFs     int
}

func NewEndOfFileController(expectedEOFs int) *EndOfFileController {
	return &EndOfFileController{
		remainingEOFsMap: NewIntMap[int](u.SerializeInt, u.DeserializeInt),
		expectedEOFs:     expectedEOFs,
	}
}

func (e *EndOfFileController) RegisterEOF(clientID int) bool {
	remainingEOFs, exists := e.remainingEOFsMap.Get(clientID)
	if !exists {
		remainingEOFs = e.expectedEOFs
	}

	remainingEOFs--
	e.remainingEOFsMap.Set(clientID, remainingEOFs)
	return remainingEOFs == 0
}

func (e *EndOfFileController) DeleteEOF(clientID int) {
	e.remainingEOFsMap.Delete(clientID)
}

func SerializeEndOfFileController(controller *EndOfFileController) []byte {
	serialized := u.SerializeInt(controller.expectedEOFs)
	serialized = append(serialized, controller.remainingEOFsMap.Serialize(controller.remainingEOFsMap)...)
	return serialized
}

func DeserializeEndOfFileController(data []byte) (*EndOfFileController, error) {
	expectedEOFs, err := u.DeserializeInt(data)
	if err != nil {
		return nil, err
	}

	remainingEOFsMapAux := NewIntMap[int](u.SerializeInt, u.DeserializeInt)
	remainingEOFsMapData := data[8:]
	remainingEOFsMap, err := remainingEOFsMapAux.Deserialize(remainingEOFsMapData)

	if err != nil {
		return nil, err
	}

	return &EndOfFileController{
		remainingEOFsMap: remainingEOFsMap,
		expectedEOFs:     expectedEOFs,
	}, nil
}
