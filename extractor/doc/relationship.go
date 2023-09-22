package doc

import (
	"dhs/util"
	"errors"
	"strconv"
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

func (ri *RelItem) GetItem(schema *Schema) (*Item, error) {
	set, err := ri.GetSet(schema)
	if err != nil {
		return &Item{}, err
	}

	return set.GetItem(ri.Item)
}

func (ri *RelItem) GetSet(schema *Schema) (*Set, error) {
	return schema.GetSet(ri.Set)
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
	return strings.ToLower(j.Parent.Stub() + "::" + j.Child.Stub())
}

type Relationship struct {
	Id        string                `json:"-"`
	Name      Name                  `json:"name"`
	Type      string                `json:"type"`
	Comment   string                `json:"comment"`
	Items     []*Join               `json:"items"`
	Integrity *ReferentialIntegrity `json:"referential_integrity"`
	Set       *Set                  `json:"-"`
}

func (r *Relationship) ID() string {
	return strings.ToLower(r.Name.Physical)
}

func (r *Relationship) Merge(rel *Relationship) error {
	if rel == nil {
		return errors.New("cannot merge a nil relationship")
	}

	if r.ID() != rel.ID() {
		return errors.New("cannot merge relationships with different names/ID")
	}

	if r.Id != rel.Id && r.Id != util.EmptyString && rel.Id != util.EmptyString && len(strings.TrimSpace(rel.Id)) > 0 && len(strings.TrimSpace(r.Id)) > 0 {
		return errors.New("cannot merge relationships with different Datahub IDs (" + r.Id + "\" & \"" + rel.Id + "\")")
	}

	if r.Type != util.EmptyString && rel.Type != util.EmptyString && r.Type != rel.Type {
		return errors.New("cannot merge different types of relationships")
	}

	if r.Set.ID() != rel.Set.ID() {
		return errors.New("cannot merge relationships from different sets")
	}

	if len(strings.TrimSpace(rel.Comment)) > 0 {
		r.Comment = rel.Comment
	}

	if r.Id == util.EmptyString && rel.Id != util.EmptyString && len(strings.TrimSpace(rel.Id)) > 0 {
		r.Id = rel.Id
	}

	if rel.Integrity != nil {
		r.Integrity = rel.Integrity
	}

	for _, item := range rel.Items {
		r.UpsertJoin(item)
	}

	return nil
}

func (r *Relationship) UpsertJoin(join *Join) *Join {
	j, err := r.GetJoin(join.ID())
	if err != nil {
		// Create New
		if r.Items == nil {
			r.Items = make([]*Join, 0)
		}
		r.Items = append(r.Items, join)
		join.Relationship = r

		r.applyJoinToSets(join)

		return join
	}

	// Update Existing
	if join.Position != util.EmptyInt && join.Position != j.Position {
		j.Position = join.Position
	}

	if join.Cardinality != util.EmptyString && join.Cardinality != j.Cardinality {
		j.Cardinality = join.Cardinality
	}

	return j
}

func (r *Relationship) applyJoinToSets(join *Join) {
	// Apply to parent set
	set, err := join.Parent.GetSet(r.Set.GetSchemaObject())
	if err == nil {
		set.LinkRelationship(r.ID())
	}

	// Apply to child set
	set, err = join.Child.GetSet(r.Set.GetSchemaObject())
	if err == nil {
		set.LinkRelationship(r.ID(), true)
	}
}

func (r *Relationship) GetJoin(id string) (*Join, error) {
	if r.Items == nil {
		r.Items = make([]*Join, 0)
		return &Join{}, errors.New("join does not exist")
	}

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

func (r *Relationship) ToPostBody() map[string]interface{} {
	if len(r.Items) == 0 {
		var empty map[string]interface{}
		return empty
	}

	cardinality := "1,1,0,-1"
	if r.Items[0].Cardinality != util.EmptyString {
		cardinality = r.Items[0].Cardinality
	}

	card := []int{}
	for _, el := range strings.Split(cardinality, ",") {
		value, _ := strconv.Atoi(strings.TrimSpace(el))
		card = append(card, int(value))
	}

	result := map[string]interface{}{
		"parent_set": r.Items[0].Parent.Schema + "." + r.Items[0].Parent.Set,
		"child_set":  r.Items[0].Child.Schema + "." + r.Items[0].Child.Set,
		"name": map[string]interface{}{
			"physical": r.Name.Physical,
		},
		"cardinality": card,
		"referential_integrity": map[string]interface{}{
			"on_update": strings.ToUpper(r.Integrity.Update),
			"on_delete": strings.ToUpper(r.Integrity.Delete),
		},
		"items": make([]map[string]interface{}, len(r.Items)),
	}

	if r.Name.Logical != util.EmptyString {
		result["name"].(map[string]interface{})["logical"] = r.Name.Logical
	}

	if r.Integrity.Match != util.EmptyString {
		result["match_type"] = strings.ToUpper(r.Integrity.Match)
	}

	for i, join := range r.Items {
		result["items"].([]map[string]interface{})[i] = map[string]interface{}{
			"parent": join.Parent.FQDN,
			"child":  join.Child.FQDN,
		}
	}

	return result
}
