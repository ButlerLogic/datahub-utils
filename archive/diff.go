package archive

import (
	"dhs/util"
	"encoding/json"
)

type Diff struct {
	Added   []interface{} `json:"add"`
	adds    []string
	Deleted []interface{} `json:"delete"`
	deletes []string
	Updated []interface{} `json:"update"`
	updates []string
}

func CreateDiff() *Diff {
	return &Diff{
		Added:   make([]interface{}, 0),
		Deleted: make([]interface{}, 0),
		Updated: make([]interface{}, 0),
	}
}

func (d *Diff) Add(i interface{}, id ...string) {
	d.Added = append(d.Added, i)
	if len(id) > 0 && id[0] != util.EmptyString {
		d.adds = append(d.adds, id[0])
	}
}

func (d *Diff) Delete(i interface{}, id ...string) {
	d.Deleted = append(d.Deleted, i)
	if len(id) > 0 && id[0] != util.EmptyString {
		d.deletes = append(d.deletes, id[0])
	}
}

func (d *Diff) Update(i interface{}, id ...string) {
	d.Updated = append(d.Updated, i)
	if len(id) > 0 && id[0] != util.EmptyString {
		d.updates = append(d.updates, id[0])
	}
}

func (d *Diff) ToJSON() []byte {
	j, _ := json.MarshalIndent(d, "", "  ")
	return j
}

func (d *Diff) HasAddition(id string) bool {
	if id == util.EmptyString {
		return false
	}

	return util.InSlice[string](id, d.adds)
}

func (d *Diff) HasDeletion(id string) bool {
	if id == util.EmptyString {
		return false
	}

	return util.InSlice[string](id, d.deletes)
}

func (d *Diff) HasUpdate(id string) bool {
	if id == util.EmptyString {
		return false
	}

	return util.InSlice[string](id, d.updates)
}
