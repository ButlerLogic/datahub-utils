package doc

type Source struct {
	Name    Name   `json:"name"`
	Comment string `json:"comment"`
	schemas map[string]*Schema
}
