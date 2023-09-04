package doc

type Name struct {
	Logical  string `json:"logical"`
	Physical string `json:"physical"`
}

type Key struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Comment string          `json:"comment"`
	Items   []*Relationship `json:"items"`
}
