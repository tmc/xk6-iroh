package perflabpeer

import (
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/gossip"
)

// GossipTopic derives the gossip topic ID for a topic name. The client
// and perflab-server (-mode gossip-member) must use the same name.
func GossipTopic(name string) gossip.TopicID {
	return gossip.TopicID(blobs.NewHash([]byte(name)))
}

// DefaultGossipTopic is the topic name used when none is configured.
const DefaultGossipTopic = "perflab-gossip-v1"
