package os_accumulator

import (
	"strconv"
)

type GameOS struct {
	AppId   uint64
	Linux   bool
	Windows bool
	Mac     bool
}

func NewGameOs(appId string, linux string, mac string, windows string) (*GameOS, error) {
	appID, err := strconv.ParseUint(appId, 10, 64)
	if err != nil {
		return nil, err
	}
	boolLinux, err := strconv.ParseBool(linux)
	if err != nil {
		return nil, err
	}
	boolMac, err := strconv.ParseBool(mac)
	if err != nil {
		return nil, err
	}
	boolWindows, err := strconv.ParseBool(windows)
	if err != nil {
		return nil, err
	}

	return &GameOS{
		AppId:   appID,
		Linux:   boolLinux,
		Windows: boolWindows,
		Mac:     boolMac,
	}, nil
}
