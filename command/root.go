// Top level commands
package command

import "github.com/alecthomas/kong"

var Root struct {
	Pull    Extractor        `cmd:"pull" short:"x" help:"Extract metadata from a datasource"`
	Push    struct{}         `cmd:"push" short:"s" help:"Syncronize meadata with the Datahub"`
	Version kong.VersionFlag `name:"version" short:"v" help:"Display the version of the application."`
}
