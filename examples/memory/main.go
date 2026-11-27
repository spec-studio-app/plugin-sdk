// The memory plugin: an example Spec Studio backend plugin that keeps
// everything in memory. With --data <file>, it also keeps its data in one
// JSON file, so the data stays after a restart. It is for tests and for
// plugin authors to read. It is not a production store.
//
//	memory                         # stdio
//	memory --data /tmp/memory.json # stdio, durable
//	memory --listen 127.0.0.1:7070 # HTTP
package main

import (
	"flag"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// version is set at release time with -ldflags "-X main.version=...".
var version = "0.0.0-dev"

func main() {
	data := flag.String("data", "", "keep the data in this JSON file")
	plugin.Main(plugin.Plugin{
		Info: protocol.PluginInfo{
			Name:    "memory",
			Version: version,
			Repo:    "https://github.com/spec-studio-app/plugin-sdk",
		},
		Store: newMemoryStore(data),
	})
}
