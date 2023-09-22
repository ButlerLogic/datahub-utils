// Top level commands
package command

import "github.com/alecthomas/kong"

var Root struct {
	Sync    Extractor        `cmd:"sync" short:"s" help:"Synchronize metadata from a data source with the Datahub"`
	Version kong.VersionFlag `name:"version" short:"v" help:"Display the version of the application."`
}
