package reviews

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type Review struct {
	AppId    uint32
	Positive bool
}

func NewReview(appId uint32, positive bool) (*Review, error) {
	return &Review{
		AppId:    appId,
		Positive: positive,
	}, nil
}

func NewReviewFromStrings(appId string, reviewScore string) (*Review, error) {
	appIdUint, err := strconv.ParseUint(appId, 10, 32)
	if err != nil {
		return nil, err
	}

	positive := reviewScore == "1"

	return &Review{
		AppId:    uint32(appIdUint),
		Positive: positive,
	}, nil
}

func (r *Review) Serialize() []byte {
	buf := make([]byte, 5)
	binary.LittleEndian.PutUint32(buf[:4], r.AppId)
	if r.Positive {
		buf[4] = 1
	} else {
		buf[4] = 0
	}
	return buf
}

func DeserializeReview(data []byte) (*Review, error) {
	if len(data) != 5 {
		return nil, fmt.Errorf("invalid data length: %d", len(data))
	}

	appId := binary.LittleEndian.Uint32(data[:4])
	positive := data[4] == 1

	return &Review{
		AppId:    appId,
		Positive: positive,
	}, nil
}
