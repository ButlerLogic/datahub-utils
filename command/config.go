package command

import (
	"dhs/util"
	"fmt"
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v3"
)

type ExtractorConfiguration struct {
	yamlfile   string
	Type       string   `yaml:"type"`
	Host       string   `yaml:"host"`
	Database   string   `yaml:"database"`
	Schemas    []string `yaml:"schemas"`
	User       string   `yaml:"user"`
	Password   string   `yaml:"password"`
	Connstr    string   `yaml:"connection_string"`
	Expand     []string `yaml:"expand_json"`
	ExpandFast bool     `yaml:"expand_fast"`
	URL        string   `yaml:"datahub_url"`
	Source     string   `yaml:"datahub_source"`
	Outfile    string   `yaml:"outfile"`
	DryRun     bool     `yaml:"dryrun"`
	System     string   `yaml:"system_id"`
	APIKey     string   `yaml:"api_key"`
	Max        int      `yaml:"max"`
	Debug      bool     `yaml:"debug"`
}

func NewConfig(path string) *ExtractorConfiguration {
	return &ExtractorConfiguration{yamlfile: path}
}

func (c ExtractorConfiguration) ConnectionString() string {
	if c.Connstr != util.EmptyString {
		return c.Connstr
	}

	switch strings.ToLower(c.Type) {
	case "postgre":
		c.Type = "postgresql"
	case "postgres":
		c.Type = "postgresql"
	case "sqlserver":
		c.Type = "mssql"
	}

	return util.EncodeURL(strings.ToLower(c.Type) + "://" + c.User + ":" + c.Password + "@" + c.Host + "/" + c.Database)
}

func (c ExtractorConfiguration) Apply(e *Extractor) error {
	yamldata, err := ioutil.ReadFile(c.yamlfile)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(yamldata, &c); err != nil {
		fmt.Printf("invalid configuration: %s", err.Error())
		return err
	}

	e.ConnectionString = c.ConnectionString()

	if e.APIKey == util.EmptyString {
		e.APIKey = c.APIKey
	}

	if e.Schemas == nil {
		e.Schemas = c.Schemas
	}

	if e.System == util.EmptyString {
		e.System = c.System
	}

	if e.Expand == nil {
		e.Expand = c.Expand
	}

	if e.Outfile == util.EmptyString {
		e.Outfile = c.Outfile
	}

	if e.DatahubURL == util.EmptyString {
		e.DatahubURL = c.URL
	}

	if e.Source == util.EmptyString {
		e.Source = c.Source
	}

	if e.SkipViewExpand == util.EmptyBool {
		e.SkipViewExpand = c.ExpandFast
	}

	if e.DryRun == util.EmptyBool && c.DryRun != util.EmptyBool {
		e.DryRun = c.DryRun
	}

	if e.Debug == util.EmptyBool && c.Debug != util.EmptyBool {
		e.Debug = c.Debug
	}

	if e.Max != util.EmptyInt && c.Max != util.EmptyInt && c.Max > 0 {
		e.Max = c.Max
	}

	if e.Max < 1 {
		e.Max = 35
	}

	return nil
}
