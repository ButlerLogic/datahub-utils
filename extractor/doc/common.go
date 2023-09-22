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
	for _, item := range key.Items {
		if !util.InSlice[string](item, k.Items) {
			k.Items = append(k.Items, item)
		}
	}

	return k
}

func (k *Key) IsPrimary() bool {
	return strings.Contains(strings.ToLower(k.Type), "primary")
}
