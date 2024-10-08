package os_accumulator

import (
	u "distribuidos-tp/internal/utils"
	"errors"
	"log"
	"strconv"
)

type GameOS struct {
	Linux   bool
	Windows bool
	Mac     bool
}

func NewGameOS(windows string, mac string, linux string) (*GameOS, error) {
	boolLinux, err := strconv.ParseBool(linux)
	if err != nil {
		log.Printf("Error parsing Linux value: %v", err)
		return nil, err
	}
	boolMac, err := strconv.ParseBool(mac)
	if err != nil {
		log.Printf("Error parsing Mac value: %v", err)
		return nil, err
	}
	boolWindows, err := strconv.ParseBool(windows)
	if err != nil {
		log.Printf("Error parsing Windows value: %v", err)
		return nil, err
	}

	return &GameOS{
		Linux:   boolLinux,
		Windows: boolWindows,
		Mac:     boolMac,
	}, nil
}

func SerializeGameOS(gameOs *GameOS) []byte {
	return []byte{
		u.ParseBoolByte(gameOs.Linux),
		u.ParseBoolByte(gameOs.Windows),
		u.ParseBoolByte(gameOs.Mac),
	}
}

func DeserializeGameOS(data []byte) (*GameOS, error) {
	if len(data) != 3 {
		return nil, errors.New("invalid data length")
	}

	linux := data[0] == 1
	windows := data[1] == 1
	mac := data[2] == 1

	return &GameOS{
		Linux:   linux,
		Windows: windows,
		Mac:     mac,
	}, nil
}
