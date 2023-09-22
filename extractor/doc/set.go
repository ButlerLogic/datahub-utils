package doc

import (
	"dhs/util"
	"errors"
	"strings"
)

type Set struct {
	Id            string                 `json:"-"`
	Name          Name                   `json:"name"`
	Schema        string                 `json:"-"`
	Comment       string                 `json:"comment"`
	Type          string                 `json:"type"`
	Items         map[string]*Item       `json:"items"`
	Relationships []string               `json:"relationships"`
	Keys          []*Key                 `json:"-"` // Re-enable this when keys are added
	schema        *Schema                `json:"-"`
	Source        string                 `json:"view_source,omitempty"`
	FQDN          string                 `json:"fqdn"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

func (set *Set) ToPostBody() map[string]interface{} {
	data := map[string]interface{}{
		"name": set.Name,
	}

	if set.Comment != util.EmptyString {
		data["description"] = set.Comment
	}

	if set.Source != util.EmptyString && len(strings.TrimSpace(set.Source)) > 0 {
		if set.Metadata == nil {
			set.Metadata = make(map[string]interface{})
		}

		set.Metadata["view_source"] = set.Source
	}

	if set.Metadata != nil {
		data["metadata"] = set.Metadata

		if val, exists := set.Metadata["most_common_value"]; exists {
			data["attributes"] = map[string]interface{}{
				"Most Common Value": val,
			}
			delete(data["metadata"].(map[string]interface{}), "most_common_value")
		}
	}

	return data
}

func (set *Set) setParent(schema *Schema) {
	set.schema = schema
	set.Schema = schema.Name.Physical
	set.FQDN = schema.Name.Physical + "." + set.Name.Physical
}

func (set *Set) GetSchemaObject() *Schema {
	return set.schema
}

var emptyName Name

func (set *Set) ID() string {
	if set == nil || set.Name == emptyName {
		return "___DOES_NOT_EXIST___"
	}

	return strings.ToLower(set.Name.Physical)
}

func (set *Set) UpsertKey(key *Key) *Key {
	for _, k := range set.Keys {
		if strings.ToLower(k.Name) == strings.ToLower(key.Name) {
			return k.Merge(key)
		}
	}

	set.Keys = append(set.Keys, key)

	return key
}

func (set *Set) GetRelationship(id string) (*Relationship, error) {
	if util.InSlice[string](id, set.Relationships) {
		if rel, exists := set.schema.Relationships[id]; exists {
			return rel, nil
		}
	}

	return &Relationship{}, errors.New(id + " relationship does not exist")
}

func (set *Set) GetRelationships() (map[string]*Relationship, error) {
	data := make(map[string]*Relationship)
	var err error
	err = nil
	for _, rel := range set.Relationships {
		if relation, exists := set.schema.Relationships[rel]; exists {
			data[rel] = relation
		} else {
			err = errors.New(rel + " relationship is referenced but does not exist")
		}
	}

	return data, err
}

func (set *Set) GetItem(name string) (*Item, error) {
	id := strings.ToLower(name)
	if item, ok := set.Items[id]; ok {
		return item, nil
	}

	return &Item{}, errors.New("Item does not exist")
}

func (set *Set) UpsertItem(item *Item) *Item {
	id := strings.ToLower(item.Name.Physical)
	currItem, err := set.GetItem(set.Name.Physical)
	if err != nil {
		if set.Items == nil {
			set.Items = make(map[string]*Item)
		}
		set.Items[id] = item
		item.setParent(set)
		return item
	}

	if len(item.Comment) > 0 {
		currItem.Comment = item.Comment
	}

	set.Items[id] = currItem

	return currItem
}

func (set *Set) LinkRelationship(id string, child ...bool) {
	prefix := "parent="
	if len(child) > 0 && child[0] {
		prefix = "child="
	}

	if set.Relationships == nil {
		set.Relationships = make([]string, 0)
	}

	if !util.InSlice[string](id, set.Relationships) {
		set.Relationships = append(set.Relationships, prefix+id)
	}
}

func (set *Set) UpsertRelationship(relation *Relationship) *Relationship {
	relation.Set = set

	if set.schema.Relationships == nil {
		set.schema.Relationships = make(map[string]*Relationship)
	}

	id := relation.ID()

	if rel, exists := set.schema.Relationships[id]; exists {
		rel.Merge(relation)
		return rel
	} else {
		set.schema.Relationships[id] = relation
		return relation
	}
}
