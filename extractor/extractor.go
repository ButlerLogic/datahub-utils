package extractor

import "dhs/extractor/doc"

type Extractor interface {
	SetConnectionString(str string) error
	Extract() (*doc.Doc, error)
	Type() string
	// Query(statement string) ([]map[string]interface{}, error)
	ExpandJSONFields(*doc.Doc, bool, ...string)
	SetDebugging(bool)
}

func GetAllSets(d *doc.Doc) []*doc.Set {
	sets := make([]*doc.Set, 0)
	for _, schema := range d.GetSchemas() {
		for _, set := range schema.Sets {
			sets = append(sets, set)
		}
	}

	return sets
}

func GetAllItems(d *doc.Doc) []*doc.Item {
	items := make([]*doc.Item, 0)
	for _, set := range GetAllSets(d) {
		for _, item := range set.Items {
			items = append(items, item)
		}
	}

	return items
}

func GetAllRelationships(d *doc.Doc) []*doc.Relationship {
	rels := make([]*doc.Relationship, 0)
	for _, schema := range d.GetSchemas() {
		for _, rel := range schema.Relationships {
			rels = append(rels, rel)
		}
	}

	return rels
}
