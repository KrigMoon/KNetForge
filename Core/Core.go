package Core

import (
	"KNetForge/Core/PeerManager"
	"KNetForge/Journal"
)

type Core struct {
	pm      PeerManager.PeerManager
	journal *Journal.Journal
}
