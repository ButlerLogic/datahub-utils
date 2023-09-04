package doc

import (
	"dhs/util"
	"errors"
	"strings"
)

type Set struct {
	Name          Name                     `json:"name"`
	Schema        string                   `json:"schema"`
	Comment       string                   `json:"comment"`
	Type          string                   `json:"type"`
	Items         map[string]*Item         `json:"items"`
	Relationships map[string]*Relationship `json:"relationships"`
	schema        *Schema                  `json:"-"`
}

func (set *Set) setParent(schema *Schema) {
	set.schema = schema
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

func (set *Set) UpsertRelationship(rel *Relationship) *Relationship {
	if set.Relationships == nil {
		set.Relationships = make(map[string]*Relationship)
	}

	id := rel.ID()
	if r, ok := set.Relationships[id]; ok {
		// Update
		if rel.Name != util.EmptyString && r.Name != rel.Name {
			r.Name = rel.Name
		}

		if r.Type != rel.Type && rel.Type != util.EmptyString {
			r.Type = rel.Type
		}

		if r.Comment != rel.Comment && rel.Comment != util.EmptyString {
			r.Comment = rel.Comment
		}

		if r.Integrity != rel.Integrity && rel.Integrity != nil {
			r.Integrity = rel.Integrity
		}

		for _, join := range rel.Items {
			r.UpsertJoin(join)
		}
	} else {
		// Create
		set.Relationships[id] = rel
	}

	return set.Relationships[id]
}
