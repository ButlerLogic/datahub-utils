package doc

import (
	"encoding/json"
	"errors"
	"strings"
)

type Doc struct {
	source  *Source
	schemas map[string]*Schema
}

func New(src *Source) *Doc {
	return &Doc{
		source:  src,
		schemas: make(map[string]*Schema),
	}
}

func (d *Doc) Source() *Source {
	return d.source
}

func (d *Doc) ApplySchema(schema *Schema) *Schema {
	currSchema, err := d.GetSchema(schema.Name.Physical)
	if err != nil {
		schema.Source = d.source
		currSchema = schema
		d.schemas[strings.ToLower(schema.Name.Physical)] = currSchema
		return currSchema
	}

	return currSchema.Merge(schema)
}

func (d *Doc) GetSchema(name string) (*Schema, error) {
	id := strings.ToLower(name)
	if schema, ok := d.schemas[id]; ok {
		return schema, nil
	}

	return &Schema{}, errors.New("schema does not exist")
}

func (d *Doc) ToJSON(minimize ...bool) []byte {
	var j []byte
	data := map[string]interface{}{
		"name":    d.source.Name,
		"schemas": d.schemas,
	}

	if len(minimize) > 0 && minimize[0] == true {
		j, _ = json.Marshal(data)
	} else {
		j, _ = json.MarshalIndent(data, "", "  ")
	}

	return j
}
