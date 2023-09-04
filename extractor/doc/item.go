package doc

type Item struct {
	Name          Name   `json:"name"`
	Comment       string `json:"comment"`
	Type          string `json:"type"`
	UDTType       string `json:"udt_type"`
	Identity      bool   `json:"identity"`
	Default       string `json:"default"`
	Nullable      bool   `json:"nullable"`
	FQDN          string `json:"fqdn"`
	relationships []*Relationship
	set           *Set
}

func (i *Item) setParent(set *Set) {
	i.set = set
}
