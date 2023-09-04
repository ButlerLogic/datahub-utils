package command

import (
	"dhs/extractor"
	"dhs/extractor/postgresql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Extractor struct {
	Config           string   `name:"config" short:"c" type:"string" help:"Specify a JSON configuration file (ignores connection string when supplied). A file called dh-config.json will be auto-recognized if it exists." default:"./dh-config.yml"`
	Schemas          []string `name:"schemas" short:"s" type:"string" help:"List of schemas to extract."`
	ConnectionString string   `arg:"conn" optional:"" help:"The connection string used to extract metadata from the data store"`
}

func (e *Extractor) Run(ctx *Context) error {
	if e.ConnectionString == "" {
		_, err := os.Stat(e.Config)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.New("configuration/connection string not found")
			}
			return err
		}

		yamldata, err := ioutil.ReadFile(e.Config)
		if err != nil {
			return err
		}

		var cfg ExtractorConfiguration
		if err := yaml.Unmarshal(yamldata, &cfg); err != nil {
			fmt.Printf("invalid configuration: %s", err.Error())
			return err
		}

		e.ConnectionString = cfg.ConnectionString()
		e.Schemas = cfg.Schemas
	}

	extractor := e.extractor()

	res, err := extractor.Extract()
	if err != nil {
		fmt.Println(err)
		return err
	}

	// TODO: Store in SQLite for processing or output as JSON
	if false {
		fmt.Println(string(res.ToJSON()))
	}

	return nil
}

func (e *Extractor) extractor() extractor.Extractor {
	schema := strings.Split(e.ConnectionString, ":")[0]

	if strings.ToLower(schema) == "postgres" || strings.ToLower(schema) == "greenplum" {
		schema = "postgresql"
	}

	var empty extractor.Extractor

	switch strings.ToLower(schema) {
	case "postgresql":
		return postgresql.New(e.ConnectionString, e.Schemas)
	case "greenplum":
		return postgresql.New(e.ConnectionString, e.Schemas)
	}

	return empty
}
