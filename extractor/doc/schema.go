package doc

import (
	"errors"
	"log"
	"strings"
)

type Schema struct {
	Id            string                   `json:"id"`
	Name          Name                     `json:"name"`
	Comment       string                   `json:"comment"`
	Relationships map[string]*Relationship `json:"relationships"`
	Sets          map[string]*Set          `json:"sets"`
	Source        *Source                  `json:"-"`
	Metadata      map[string]interface{}   `json:"metadata,omitempty"`
}

func (s *Schema) ID() string {
	return strings.ToLower(s.Name.Physical)
}

// Merge a schema
func (s *Schema) Merge(schema *Schema) *Schema {
	if len(strings.TrimSpace(s.Comment)) == 0 && len(strings.TrimSpace(schema.Comment)) > 0 {
		s.Comment = schema.Comment
	}

	for _, set := range schema.Sets {
		s.UpsertSet(set)
	}

	return s
}

func (s *Schema) GetSet(name string) (*Set, error) {
	id := strings.ToLower(name)
	if set, exists := s.Sets[id]; exists {
		return set, nil
	}

	return &Set{Name: Name{Physical: name}}, errors.New(name + " set does not exist")
}

func (s *Schema) UpsertSet(set *Set) *Set {
	id := set.ID()
	currSet, err := s.GetSet(id)
	if err != nil {
		set.setParent(s)
		s.Sets[id] = set
		set.Relationships = make([]string, 0)
		return set
	}

	if len(set.Comment) > 0 {
		currSet.Comment = set.Comment
	}

	s.Sets[id] = currSet

	return currSet
}

func (s *Schema) GetViews() []*Set {
	sets := make([]*Set, 0)

	for _, set := range s.Sets {
		if strings.Contains(strings.ToUpper(set.Type), "VIEW") {
			sets = append(sets, set)
		}
	}

	return sets
}

func (s *Schema) GetNonViews() []*Set {
	sets := make([]*Set, 0)

	for _, set := range s.Sets {
		if !strings.Contains(strings.ToUpper(set.Type), "VIEW") {
			sets = append(sets, set)
		}
	}

	return sets
}

func (s *Schema) UpsertRelationship(rel *Relationship) *Relationship {
	if s.Relationships == nil {
		s.Relationships = make(map[string]*Relationship)
	}

	id := rel.ID()
	if r, exists := s.Relationships[id]; exists {
		r.Merge(rel)
		return r
	}

	s.Relationships[id] = rel

	// Link parent set to relationship
	rel.Set.LinkRelationship(rel.ID())

	// Link child sets to relationship
	for _, join := range rel.Items {
		set, err := join.Child.GetSet(s)
		if err != nil {
			log.Println(err)
		} else {
			set.LinkRelationship(rel.ID())
		}
	}

	return rel
}
