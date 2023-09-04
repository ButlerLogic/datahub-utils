package command

import (
	"errors"
	"strings"
)

type Context struct {
	Debug bool
	meta  map[string]interface{}
}

func (c *Context) Set(name string, value interface{}) {
	if c.meta == nil {
		c.meta = make(map[string]interface{})
	}

	c.meta[strings.ToLower(name)] = value
}

func (c *Context) Get(name string) (interface{}, error) {
	if c.meta == nil {
		c.meta = make(map[string]interface{})
	}

	if value, ok := c.meta[strings.ToLower(name)]; ok {
		return value, nil
	}

	return nil, errors.New(name + " does not exist in the context")
}
