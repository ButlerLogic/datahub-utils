package doc

import (
	"dhs/util"
	"strings"
)

type Item struct {
	Id       string                 `json:"-"`
	Name     Name                   `json:"name"`
	Comment  string                 `json:"comment"`
	Type     string                 `json:"type"`
	UDTType  string                 `json:"udt_type"`
	Identity bool                   `json:"identity"`
	Default  string                 `json:"default"`
	Nullable bool                   `json:"nullable"`
	FQDN     string                 `json:"fqdn"`
	Example  string                 `json:"example,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Key      *Key                   `json:"key,omitempty"`
	set      *Set                   `json:"-"`
}

func (i *Item) ToPostBody() map[string]interface{} {
	data := map[string]interface{}{
		"name": i.Name,
	}

	if i.Comment != util.EmptyString {
		data["description"] = i.Comment
	}

	if i.Type != util.EmptyString {
		data["udt_type"] = i.Type
	}

	if i.Default != util.EmptyString {
		data["default"] = i.Default
	}

	if i.Nullable != util.EmptyBool {
		data["nullable"] = i.Nullable
	}

	if i.Key != nil {
		data["key"] = make(map[string]interface{})
		data["key"].(map[string]interface{})["name"] = i.Key.Name
		data["key"].(map[string]interface{})["primary"] = i.Key.IsPrimary()
	}

	if i.Metadata != nil {
		data["metadata"] = i.Metadata
		if i.Metadata["most_common_value"] != nil {
			if data["metadata"].(map[string]interface{})["attributes"] == nil {
				data["metadata"].(map[string]interface{})["attributes"] = make(map[string]interface{})
			}

			data["metadata"].(map[string]interface{})["attributes"].(map[string]interface{})["Most Common Value"] = i.Metadata["most_common_value"]
			delete(data["metadata"].(map[string]interface{}), "most_common_value")
		}
		if i.Metadata["null_percentage"] != nil {
			if data["metadata"].(map[string]interface{})["attributes"] == nil {
				data["metadata"].(map[string]interface{})["attributes"] = make(map[string]interface{})
			}

			data["metadata"].(map[string]interface{})["attributes"].(map[string]interface{})["Null Percentage"] = i.Metadata["null_percentage"]
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
