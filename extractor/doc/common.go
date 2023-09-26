package doc

import (
	"dhs/util"
	"strings"
)

type Name struct {
	Logical  string `json:"logical,omitempty"`
	Physical string `json:"physical"`
}

type Key struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Comment string   `json:"comment"`
	Items   []string `json:"items,omitempty"`
}

func (k *Key) Merge(key *Key) *Key {
	if k == nil {
		k = &Key{}
		k.Name = key.Name
		k.Type = key.Type
		k.Comment = key.Comment
		k.Items = key.Items
		return k
	}

	for _, item := range key.Items {
		if !util.InSlice[string](item, k.Items) {
			k.Items = append(k.Items, item)
		}
	}

	if k.Type == util.EmptyString || len(k.Type) == 0 {
		if key.Type != util.EmptyString && len(key.Type) > 0 {
			k.Type = key.Type
		}
	}

	if (k.Comment == util.EmptyString || len(k.Comment) > 0) && key.Comment != util.EmptyString && len(key.Comment) > 0 {
		k.Comment = key.Comment
	}

	return k
}

func (k *Key) IsPrimary() bool {
	if k == nil || k.Type == util.EmptyString {
		return false
	}

	return strings.Contains(strings.ToLower(k.Type), "primary")
}
