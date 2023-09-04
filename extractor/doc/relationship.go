package doc

import (
	"dhs/util"
	"errors"
	"strings"
)

type RelItem struct {
	Schema string `json:"schema"`
	Set    string `json:"set"`
	Item   string `json:"item"`
	FQDN   string `json:"fqdn"`
}

func (ri *RelItem) Stub() string {
	if len(ri.FQDN) > 0 {
		return strings.ToLower(ri.FQDN)
	}

	return strings.ToLower(ri.Schema + "." + ri.Set + "." + ri.Item)
}

type ReferentialIntegrity struct {
	Update string `json:"on_update"`
	Delete string `json:"on_delete"`
	Match  string `json:"on_match"`
}

type Join struct {
	Parent       *RelItem      `json:"parent"`
	Child        *RelItem      `json:"child"`
	Position     int           `json:"position"`
	Cardinality  string        `json:"cardinality"`
	Relationship *Relationship `json:"-"`
}

func (j *Join) ID() string {
	return j.Parent.Stub() + "::" + j.Child.Stub()
}

type Relationship struct {
	Name      string                `json:"name"`
	Type      string                `json:"type"`
	Comment   string                `json:"comment"`
	Items     []*Join               `json:"items"`
	Integrity *ReferentialIntegrity `json:"referential_integrity"`
	Set       *Set                  `json:"-"`
}

func (r *Relationship) ID() string {
	return strings.ToLower(r.Name)
}

func (r *Relationship) UpsertJoin(join *Join) *Join {
	j, err := r.GetJoin(join.ID())
	if err != nil {
		if r.Items == nil {
			r.Items = make([]*Join, 0)
		}
		r.Items = append(r.Items, join)
		join.Relationship = r
		return join
	}

	if join.Position != util.EmptyInt && join.Position != j.Position {
		j.Position = join.Position
	}

	if join.Cardinality != util.EmptyString && join.Cardinality != j.Cardinality {
		j.Cardinality = join.Cardinality
	}

	return j
}

func (r *Relationship) GetJoin(id string) (*Join, error) {
	for _, join := range r.Items {
		if join.ID() == id {
			return join, nil
		}
	}

	return &Join{}, errors.New("join does not exist")
}

func (r *Relationship) HasJoin(id string) bool {
	_, err := r.GetJoin(id)
	if err != nil {
		return false
	}

	return true
}
