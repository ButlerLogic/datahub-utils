package command

import (
	"dhs/util"
	"strings"
)

type ExtractorConfiguration struct {
	Type     string   `yaml:"type"`
	Host     string   `yaml:"host"`
	Database string   `yaml:"database"`
	Schemas  []string `yaml:"schemas"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Connstr  string   `yaml:"connection_string"`
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

	return strings.ToLower(c.Type) + "://" + c.User + ":" + c.Password + "@" + c.Host + "/" + c.Database
}
