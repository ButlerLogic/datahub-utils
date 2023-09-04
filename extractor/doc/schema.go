package doc

import (
	"errors"
	"fmt"
	"strings"
)

type Schema struct {
	Name    Name            `json:"name"`
	Comment string          `json:"comment"`
	Sets    map[string]*Set `json:"sets"`
	Source  *Source         `json:"-"`
}

// Merge a schema
func (s *Schema) Merge(schema *Schema) *Schema {
	if schema.Comment != s.Comment && len(s.Comment) == 0 {
		s.Comment = schema.Comment
	}

	// TODO: Loop through sets and add/remove/update
	fmt.Printf("==================\nTODO: Merge sets into schemas\n==================\n")

	return s
}

func (s *Schema) GetSet(name string) (*Set, error) {
	id := strings.ToLower(name)
	if set, ok := s.Sets[id]; ok {
		return set, nil
	}

	return &Set{}, errors.New("Set does not exist")
}

func (s *Schema) UpsertSet(set *Set) *Set {
	id := strings.ToLower(set.Name.Physical)
	currSet, err := s.GetSet(set.Name.Physical)
	if err != nil {
		s.Sets[id] = set
		set.setParent(s)
		if set.Relationships == nil {
			set.Relationships = make(map[string]*Relationship)
		}
		return set
	}

	if len(set.Comment) > 0 {
		currSet.Comment = set.Comment
	}

	s.Sets[id] = currSet

	return currSet
}
