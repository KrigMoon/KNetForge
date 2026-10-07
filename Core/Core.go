package Core

import (
	"github.com/KrigMoon/KNetForge/Core/PeerManager"
	"github.com/KrigMoon/KNetForge/Journal"
)

type Core struct {
	pm      PeerManager.PeerManager
	journal *Journal.Journal
}
