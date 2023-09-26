package doc

import (
	"dhs/util"
	"fmt"
	"strconv"
	"strings"
)

type Item struct {
	Id      string `json:"-"`
	Name    Name   `json:"name"`
	Comment string `json:"comment"`
	Type    string `json:"udt_type"`
	// UDTType      string                 `json:"udt_type"`
	Identity     bool                   `json:"identity"`
	Default      string                 `json:"default"`
	Nullable     bool                   `json:"nullable"`
	FQDN         string                 `json:"fqdn"`
	Example      string                 `json:"example,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	Keys         map[string]*Key        `json:"keys,omitempty"`
	set          *Set                   `json:"-"`
	UpdateFields []string               `json:"-"`
}

func (i *Item) ToPostBody() map[string]interface{} {
	data := map[string]interface{}{
		"name": i.Name,
	}

	data["nullable"] = i.Nullable
	if data["nullable"] == nil {
		delete(data, "nullable")
	}

	if i.Comment != util.EmptyString {
		data["description"] = i.Comment
	}

	if i.Type != util.EmptyString {
		data["udt_type"] = i.Type
	}

	if i.Set().Name.Physical == "user_group" && i.Name.Physical == "seqid" {
		fmt.Printf("==> %v | %v > %v\n", (i.Default != util.EmptyString), i.Default, util.EmptyString)
	}
	if i.Default != util.EmptyString {
		data["default"] = i.Default
	}

	if i.Keys != nil && len(i.Keys) > 0 {
		d := make([]map[string]interface{}, 0)
		for _, k := range i.Keys {
			if k != nil {
				if k.Name != util.EmptyString && len(strings.TrimSpace(k.Name)) > 0 {
					d = append(d, map[string]interface{}{
						"name":    k.Name,
						"primary": k.IsPrimary(),
					})
				}
			}
		}

		if len(d) > 0 {
			data["keys"] = d
		}
	}

	if i.Metadata != nil {
		data["metadata"] = i.Metadata
		if i.Metadata["most_common_value"] != nil {
			if data["attributes"] == nil {
				data["attributes"] = make(map[string]interface{})
			}

			data["attributes"].(map[string]interface{})["Most Common Value"] = i.Metadata["most_common_value"].(string)
			delete(data["metadata"].(map[string]interface{}), "most_common_value")
		}
		if i.Metadata["null_percentage"] != nil {
			if data["attributes"] == nil {
				data["attributes"] = make(map[string]interface{})
			}

			if int(i.Metadata["null_percentage"].(float64)) == int(0) {
				data["attributes"].(map[string]interface{})["Null Percentage"] = "0"
			} else {
				data["attributes"].(map[string]interface{})["Null Percentage"] = strconv.FormatFloat(i.Metadata["null_percentage"].(float64), 'f', 2, 64)
			}
			delete(data["metadata"].(map[string]interface{}), "null_percentage")
		}
	}

	if i.Example != util.EmptyString {
		data["example"] = i.Example
	}

	return data
}

func (i *Item) ID() string {
	return strings.ToLower(strings.TrimSpace(i.FQDN))
}

func (i *Item) setParent(set *Set) {
	i.set = set
}

func (i *Item) ApplySet(set *Set) {
	i.set = set
}

func (i *Item) Set() *Set {
	return i.set
}

func (i *Item) SetFQDN() string {
	return i.set.FQDN
}

func (i *Item) UpsertKey(key *Key) *Key {
	k, exists := i.Keys[strings.ToLower(key.Name)]

	if exists {
		k.Merge(key)
	} else {
		if i.Keys == nil {
			i.Keys = make(map[string]*Key)
		}
		i.Keys[strings.ToLower(key.Name)] = k
	}

	return k
}

var emptykey *Key

func (i *Item) GetKey(name string) *Key {
	if i.Keys == nil || len(i.Keys) == 0 {
		return emptykey
	}

	if key, exists := i.Keys[name]; exists {
		return key
	}

	return emptykey
}

func (i *Item) IsPrimaryKey() (bool, string) {
	for _, key := range i.Keys {
		if key.IsPrimary() {
			return true, key.Name
		}
	}

	return false, util.EmptyString
}
