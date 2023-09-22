package doc

import (
	"dhs/util"
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
	currentSchema, err := d.GetSchema(schema.Name.Physical)
	if err != nil {
		schema.Source = d.source
		d.schemas[schema.ID()] = schema
		return schema
	}

	return currentSchema.Merge(schema)
}

func (d *Doc) GetSchema(name string) (*Schema, error) {
	id := strings.ToLower(name)
	if schema, exists := d.schemas[id]; exists {
		return schema, nil
	}

	return &Schema{Name: Name{Physical: name}}, errors.New(name + " schema does not exist")
}

func (d *Doc) GetSchemas() []*Schema {
	result := make([]*Schema, 0)
	for _, s := range d.schemas {
		result = append(result, s)
	}

	return result
}

func (d *Doc) ToJSON(minimize ...bool) []byte {
	var j []byte
	data := map[string]interface{}{
		"name":    d.source.Name,
		"schemas": d.schemas,
	}

	if d.source.Comment != util.EmptyString {
		data["comment"] = d.source.Comment
	}

	if len(minimize) > 0 && minimize[0] == true {
		j, _ = json.Marshal(data)
	} else {
		j, _ = json.MarshalIndent(data, "", "  ")
	}

	return j
}

func (d *Doc) GetViews(schemaName string) ([]*Set, error) {
	if schema, exists := d.schemas[schemaName]; exists {
		return schema.GetViews(), nil
	}

	return make([]*Set, 0), errors.New(schemaName + " schema not found")
}

func (d *Doc) GetItemsByType(types ...string) map[string][]*Item {
	result := map[string][]*Item{}

	list := make([]string, len(types))
	for i, value := range types {
		list[i] = strings.ToLower(value)
		result[strings.ToLower(value)] = make([]*Item, 0)
	}

	if d.schemas == nil {
		return result
	}

	for _, schema := range d.schemas {
		for _, set := range schema.Sets {
			for _, item := range set.Items {
				t := strings.ToLower(item.Type)
				if util.InSlice[string](t, list) {
					result[t] = append(result[t], item)
				}
			}
		}
	}

	return result
}
