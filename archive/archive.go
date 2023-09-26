package archive

import (
	"database/sql"
	"dhs/extractor/doc"
	"dhs/util"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type Archive struct {
	path string
	doc  *doc.Doc
}

//go:embed metadoc.db
var DB_TEMPLATE embed.FS

//go:embed sql/sets_new.sql
var ADD_SET_SQL string

//go:embed sql/sets_deleted.sql
var DELETE_SET_SQL string

//go:embed sql/sets_updated.sql
var UPDATE_SET_SQL string

//go:embed sql/items_new.sql
var ADD_ITEM_SQL string

//go:embed sql/items_deleted.sql
var DELETE_ITEM_SQL string

//go:embed sql/items_updated.sql
var UPDATE_ITEM_SQL string

//go:embed sql/relationships_new.sql
var ADD_RELATIONSHIP_SQL string

//go:embed sql/relationships_deleted.sql
var DELETE_RELATIONSHIP_SQL string

//go:embed sql/relationships_updated.sql
var UPDATE_RELATIONSHIP_SQL string

//go:embed sql/joins_new.sql
var ADD_JOIN_SQL string

//go:embed sql/joins_deleted.sql
var DELETE_JOIN_SQL string

//go:embed sql/joins_updated.sql
var UPDATE_JOIN_SQL string

func Open(path string, d ...*doc.Doc) *Archive {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			data, err := DB_TEMPLATE.ReadFile("metadoc.db")
			if err != nil {
				log.Fatal(err.Error())
			}

			err = ioutil.WriteFile(path, data, 0644)
			if err != nil {
				log.Fatalf("Error writing to file: %v", err)
			}
		} else {
			log.Fatal(err.Error())
		}
	}

	var document *doc.Doc
	if len(d) > 0 {
		document = d[0]
	}

	return &Archive{path: path, doc: document}
}

func (a *Archive) Doc() *doc.Doc {
	return a.doc
}

func (a *Archive) SetDoc(d *doc.Doc) {
	a.doc = d
}

func (a *Archive) HasDoc() bool {
	var empty *doc.Doc
	if a.doc == empty {
		return false
	}

	return true
}

func (a *Archive) Query(statement string) (*RecordSet, error) {
	conn, err := sql.Open("sqlite3", a.path)
	if err != nil {
		return &RecordSet{}, errors.New("error connecting to embedded archive data: " + err.Error())
	}
	defer conn.Close()

	var rows *sql.Rows
	sql := strings.ToUpper(strings.TrimSpace(statement))
	if strings.Contains(sql, "SELECT ") {
		rows, err = conn.Query(statement)
		if err != nil {
			return &RecordSet{}, err
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return &RecordSet{}, err
		}

		// Create a slice of interface{} to hold the column values
		values := make([]interface{}, len(columns))
		for i := range values {
			var value interface{}
			values[i] = &value
		}

		// Iterate over the rows and fetch data dynamically
		results := createRecordSet()
		for rows.Next() {
			err := rows.Scan(values...)
			if err != nil {
				return &RecordSet{}, err
			}

			// Print the values dynamically
			record := map[string]interface{}{}
			for i, col := range columns {
				val := *values[i].(*interface{})
				record[col] = val
			}

			results.Add(record)
		}

		return results, rows.Err()
	}

	_, err = conn.Exec(statement)
	return &RecordSet{}, err
}

func (a *Archive) UpsertSets(srctype string, sets []*doc.Set) error {
	srctype = strings.ToLower(strings.TrimSpace(srctype))
	if srctype != "source" && srctype != "datahub" {
		return errors.New("Sets can only be applied to \"source\" or \"datahub\" archives.")
	}

	prefix := "db_"
	if srctype == "datahub" {
		prefix = "dh_"
	}

	sql := []string{}
	for _, set := range sets {
		if srctype == "datahub" {
			sql = append(sql, fmt.Sprintf("('%s','%s','%s','%s','%s','%s','%s')", strings.ReplaceAll(set.Name.Physical, "'", "''"), strings.ReplaceAll(set.Name.Logical, "'", "''"), strings.ReplaceAll(set.Schema, "'", "''"), strings.ReplaceAll(set.Comment, "'", "''"), strings.ReplaceAll(set.Type, "'", "''"), strings.ReplaceAll(set.Source, "'", "''"), strings.ReplaceAll(set.Id, "'", "''")))
		} else {
			sql = append(sql, fmt.Sprintf("('%s','%s','%s','%s','%s','%s')", strings.ReplaceAll(set.Name.Physical, "'", "''"), strings.ReplaceAll(set.Name.Logical, "'", "''"), strings.ReplaceAll(set.Schema, "'", "''"), strings.ReplaceAll(set.Comment, "'", "''"), strings.ReplaceAll(set.Type, "'", "''"), strings.ReplaceAll(set.Source, "'", "''")))
		}
	}

	isdh := ""
	if srctype == "datahub" {
		isdh = ", id"
	}

	size := 1000
	for i := 0; i < len(sql); i += size {
		end := 1 + size
		if end > len(sql) {
			end = len(sql)
		}
		chunk := sql[i:end]

		statement := "INSERT OR REPLACE INTO " + prefix + "dataset (physical_nm, logical_nm, schema, description, type, definition" + isdh + ")\nVALUES\n  " + strings.Join(chunk, ",\n  ") + ";"
		_, err := a.Query(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Archive) ResetDatahub() {
	sql := []string{
		"DELETE FROM dh_dataset;",
		"DELETE FROM dh_dataitem;",
		"DELETE FROM dh_relationship;",
		"DELETE FROM dh_join;",
	}

	for _, stmt := range sql {
		_, err := a.Query(stmt)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (a *Archive) ResetDatasource() {
	sql := []string{
		"DELETE FROM db_dataset;",
		"DELETE FROM db_dataitem;",
		"DELETE FROM db_relationship;",
		"DELETE FROM db_join;",
	}

	for _, stmt := range sql {
		_, err := a.Query(stmt)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (a *Archive) Reset() {
	a.ResetDatahub()
	a.ResetDatasource()
	a.doc = doc.New(a.doc.Source())
}

func (a *Archive) UpsertItems(srctype string, items []*doc.Item) error {
	srctype = strings.ToLower(strings.TrimSpace(srctype))
	if srctype != "source" && srctype != "datahub" {
		return errors.New("Sets can only be applied to \"source\" or \"datahub\" archives.")
	}

	prefix := "db_"
	if srctype == "datahub" {
		prefix = "dh_"
	}

	sql := []string{}
	for _, item := range items {
		ispk, keynm := item.IsPrimaryKey()

		var nullable bool
		if item.Nullable != util.EmptyBool {
			nullable = item.Nullable
		}

		var example string
		if item.Example != util.EmptyString {
			example = strings.ReplaceAll(item.Example, "'", "''")
		}

		var defaultVal string
		if item.Default != util.EmptyString {
			defaultVal = strings.ReplaceAll(item.Default, "'", "''")
		}

		meta := ", NULL"
		if item.Metadata != nil {
			j, _ := json.Marshal(item.Metadata)
			meta = ", '" + strings.ReplaceAll(string(j), "'", "''") + "'"
		}

		if srctype == "datahub" {
			sql = append(sql, fmt.Sprintf("('%s','%s','%s','%s','%s',%t,'%s',%t,'%s','%s'%s,'%s')",
				strings.ReplaceAll(item.Set().Name.Physical, "'", "''"),
				strings.ReplaceAll(item.Name.Physical, "'", "''"),
				strings.ReplaceAll(item.Name.Logical, "'", "''"),
				strings.ReplaceAll(item.Type, "'", "''"),
				strings.ReplaceAll(item.Comment, "'", "''"),
				ispk,
				keynm,
				nullable,
				example,
				defaultVal,
				meta,
				strings.ReplaceAll(item.Id, "'", "''"),
			))
		} else {
			sql = append(sql, fmt.Sprintf("('%s','%s','%s','%s','%s',%t,'%s',%t,'%s','%s'%s)",
				strings.ReplaceAll(item.Set().Name.Physical, "'", "''"),
				strings.ReplaceAll(item.Name.Physical, "'", "''"),
				strings.ReplaceAll(item.Name.Logical, "'", "''"),
				strings.ReplaceAll(item.Type, "'", "''"),
				strings.ReplaceAll(item.Comment, "'", "''"),
				ispk,
				keynm,
				nullable,
				example,
				defaultVal,
				meta,
			))
		}
	}

	size := 1000
	for i := 0; i < len(sql); i += size {
		end := 1 + size
		if end > len(sql) {
			end = len(sql)
		}
		chunk := sql[i:end]

		isdh := ""
		if srctype == "datahub" {
			isdh = ", id"
		}
		statement := "INSERT OR REPLACE INTO " + prefix + "dataitem (dataset_id, physical_nm, logical_nm, type, description, is_pk, key_nm, nullable, example, default_val, metadata" + isdh + ")\nVALUES\n  " + strings.Join(chunk, ",\n  ") + ";"
		_, err := a.Query(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Archive) UpsertRelationships(srctype string, rels []*doc.Relationship) error {
	srctype = strings.ToLower(strings.TrimSpace(srctype))
	if srctype != "source" && srctype != "datahub" {
		return errors.New("Relationships can only be applied to \"source\" or \"datahub\" archives.")
	}

	prefix := "db_"
	if srctype == "datahub" {
		prefix = "dh_"
	}

	sql := []string{}
	joinsql := []string{}
	for _, rel := range rels {
		if len(rel.Items) > 0 {
			if srctype == "datahub" {
				sql = append(sql, fmt.Sprintf("('%s','%s','%s','%s','%s','%s','%s','%s','%s')",
					strings.ReplaceAll(rel.Name.Physical, "'", "''"),
					strings.ReplaceAll(rel.Set.Name.Physical, "'", "''"),
					strings.ReplaceAll(rel.Name.Logical, "'", "''"),
					strings.ReplaceAll(rel.Type, "'", "''"),
					strings.ReplaceAll(rel.Comment, "'", "''"),
					rel.Integrity.Update,
					rel.Integrity.Delete,
					rel.Integrity.Match,
					rel.Id,
				))
			} else {
				sql = append(sql, fmt.Sprintf("('%s','%s','%s','%s','%s','%s','%s','%s')",
					strings.ReplaceAll(rel.Name.Physical, "'", "''"),
					strings.ReplaceAll(rel.Set.Name.Physical, "'", "''"),
					strings.ReplaceAll(rel.Name.Logical, "'", "''"),
					strings.ReplaceAll(rel.Type, "'", "''"),
					strings.ReplaceAll(rel.Comment, "'", "''"),
					rel.Integrity.Update,
					rel.Integrity.Delete,
					rel.Integrity.Match,
				))
			}

			for _, join := range rel.Items {
				pos := 0
				if join.Position != util.EmptyInt {
					pos = join.Position
				}

				joinsql = append(joinsql, fmt.Sprintf("('%s','%s','%s',%v,'%s')",
					strings.ReplaceAll(rel.Name.Physical, "'", "''"),
					strings.ReplaceAll(join.Parent.FQDN, "'", "''"),
					strings.ReplaceAll(join.Child.FQDN, "'", "''"),
					pos,
					strings.ReplaceAll(join.Cardinality, "'", "''"),
				))
			}
		}
	}

	size := 1000
	for i := 0; i < len(sql); i += size {
		end := 1 + size
		if end > len(sql) {
			end = len(sql)
		}
		chunk := sql[i:end]

		isdh := ""
		if srctype == "datahub" {
			isdh = ", id"
		}

		statement := "INSERT OR REPLACE INTO " + prefix + "relationship (physical_nm, dataset_id, logical_nm, type, comment, on_update, on_delete, on_match" + isdh + ")\nVALUES\n  " + strings.Join(chunk, ",\n  ") + ";"
		_, err := a.Query(statement)
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(joinsql); i += size {
		end := 1 + size
		if end > len(joinsql) {
			end = len(joinsql)
		}
		chunk := joinsql[i:end]

		statement := "INSERT OR REPLACE INTO " + prefix + "join (db_relationship_id, parent_fqdn, child_fqdn, position, cardinality)\nVALUES\n  " + strings.Join(chunk, ",\n  ") + ";"
		_, err := a.Query(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Archive) DiffSets() (*Diff, error) {
	d := CreateDiff()

	rs, err := a.Query(ADD_SET_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		set, err := a.getSet(record)
		if err == nil {
			// j, _ := json.MarshalIndent(record, "", "  ")
			// fmt.Println(string(j))

			if record["description"] != nil && len(record["description"].(string)) > 0 && (set.Comment == util.EmptyString || len(strings.TrimSpace(set.Comment)) == 0) {
				set.Comment = record["description"].(string)
			}

			if record["type"] != nil && len(strings.TrimSpace(record["type"].(string))) > 0 {
				set.Type = record["type"].(string)
			}

			if record["definition"] != nil && len(record["definition"].(string)) > 0 {
				set.Source = record["definition"].(string)
			}

			d.Add(set, set.ID())
		} else {
			fmt.Printf("WARNING: Failed to identify set -> %v\n", err.Error())
			j, _ := json.MarshalIndent(record, "", "  ")
			fmt.Println(string(j))
		}

		return nil
	})

	rs, err = a.Query(DELETE_SET_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		set, err := a.getSet(record)
		if err == nil {
			d.Delete(set, set.ID())
		} else if strings.Contains(err.Error(), "set does not exist") {
			schema, err := a.doc.GetSchema(record["schema"].(string))
			if err != nil {
				return err
			}

			set := schema.UpsertSet(&doc.Set{
				Id: record["id"].(string),
				Name: doc.Name{
					Physical: record["physical_nm"].(string),
					Logical:  record["logical_nm"].(string),
				},
				Comment: record["description"].(string),
				FQDN:    schema.Name.Physical + "." + record["physical_nm"].(string),
			})

			d.Delete(set, set.ID())
		} else {
			fmt.Printf("WARNING: %v\n", err.Error())
			j, _ := json.MarshalIndent(record, "", "  ")
			fmt.Println(string(j))
		}

		return nil
	})

	rs, err = a.Query(UPDATE_SET_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		set, err := a.getSet(record)
		if err == nil {
			if record["id"] != nil {
				set.Id = record["id"].(string)
			}

			d.Update(set)
		} else {
			fmt.Println(err)
		}

		return nil
	})

	return d, nil
}

func (a *Archive) DiffItems(setdiff *Diff) (*Diff, error) {
	d := CreateDiff()

	rs, err := a.Query(ADD_ITEM_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		set, err := a.getSet(record)
		if err == nil {
			item, err := set.GetItem(record["physical_nm"].(string))
			if err == nil {
				d.Add(item, item.ID())
			} else {
				item := set.UpsertItem(&doc.Item{
					Name: doc.Name{
						Physical: record["physical_nm"].(string),
					},
					// Type: getType(),
					Type: record["type"].(string),
					// UDTType:  record["type"].(string),
					Nullable: record["nullable"].(bool),
					FQDN:     set.ID() + "." + record["physical_nm"].(string),
				})

				if record["logical_nm"] != nil && len(strings.TrimSpace(record["logical_nm"].(string))) > 0 {
					item.Name.Logical = record["logical_nm"].(string)
				}

				if record["description"] != nil && len(strings.TrimSpace(record["description"].(string))) > 0 {
					item.Comment = record["description"].(string)
				}

				if record["metadata"] != nil {
					switch value := record["metadata"].(type) {
					case string:
						var md map[string]interface{}
						json.Unmarshal([]byte(value), &md)
						record["metadata"] = md
					}

					item.Metadata = record["metadata"].(map[string]interface{})
				}

				if record["key"] != nil {
					k := record["key"].(map[string]interface{})

					if k["is_key"].(bool) {
						var t string
						if k["primary"].(bool) {
							t = "primary"
						} else {
							t = "foreign"
						}

						item.UpsertKey(&doc.Key{
							Name:  k["name"].(string),
							Type:  t,
							Items: []string{item.Name.Physical},
						})
					}
				}

				d.Add(item)
				// fmt.Println(">>>> " + err.Error())
				// j, _ := json.MarshalIndent(record, "", "  ")
				// fmt.Println(string(j))
			}
		} else {
			fmt.Printf("WARNING: Failed to identify item parent set (%v) -> %v\n", record["dataset_id"], err.Error())
			j, _ := json.MarshalIndent(record, "", "  ")
			fmt.Println(string(j))
		}

		return nil
	})

	deadsets := make([]string, len(setdiff.Deleted))
	for i, set := range setdiff.Deleted {
		deadsets[i] = set.(*doc.Set).Name.Physical
	}

	rs, err = a.Query(DELETE_ITEM_SQL)
	if err != nil {
		return d, err
	}

	deleted_items := make([]string, 0)
	rs.ForEach(func(record map[string]interface{}) error {
		if len(deadsets) == 0 || !util.InSlice[string](record["dataset_id"].(string), deadsets) {
			set, err := a.getSet(record)
			if err == nil {
				item, err := set.GetItem(record["physical_nm"].(string))
				if err == nil {
					d.Delete(item, item.ID())
					deleted_items = append(deleted_items, item.FQDN)
				} else {
					schema, err := a.doc.GetSchema(record["schema"].(string))
					if err != nil {
						return err
					}

					newset := schema.UpsertSet(&doc.Set{
						Id: record["dataset_id"].(string),
						Name: doc.Name{
							Physical: record["dataset_id"].(string),
						},
						FQDN: schema.Name.Physical + "." + record["dataset_id"].(string),
					})

					item := newset.UpsertItem(&doc.Item{
						Id: record["id"].(string),
						Name: doc.Name{
							Physical: record["physical_nm"].(string),
							Logical:  record["logical_nm"].(string),
						},
						Comment:  record["description"].(string),
						Type:     record["type"].(string),
						Default:  record["default_val"].(string),
						Nullable: record["nullable"].(bool),
						FQDN:     strings.ToLower(set.Name.Physical) + "." + strings.ToLower(record["physical_nm"].(string)),
						Example:  record["example"].(string),
					})

					item.ApplySet(newset)

					if record["metadata"] != nil {
						var data map[string]interface{}
						json.Unmarshal([]byte(record["metadata"].(string)), &data)
						item.Metadata = data
					}

					if record["is_pk"].(bool) || (record["key_nm"] != nil && len(record["key_nm"].(string)) > 0) {
						keytype := "foreign"
						if record["is_pk"].(bool) {
							keytype = "primary"
						}

						item.UpsertKey(&doc.Key{
							Name: record["key_nm"].(string),
							Type: keytype,
						})
					}

					d.Delete(item, item.ID())

					deleted_items = append(deleted_items, item.FQDN)
				}
			} else {
				fmt.Printf("WARNING: Failed to identify item parent set (%v) -> %v\n", record["dataset_id"], err.Error())
				j, _ := json.MarshalIndent(record, "", "  ")
				fmt.Println(string(j))
			}
		}

		return nil
	})

	rs, err = a.Query(UPDATE_ITEM_SQL)
	if err != nil {
		fmt.Println(err)
		return d, err
	}

	var emptykey *doc.Key

	rs.ForEach(func(record map[string]interface{}) error {
		if !util.InSlice[string](record["dataset_id"].(string), deadsets) {
			set, err := a.getSet(record)
			set.Id = record["set_id"].(string)
			if err == nil {
				item, err := set.GetItem(record["physical_nm"].(string))
				if err == nil {
					if !util.InSlice[string](item.FQDN, deleted_items) {
						item.Id = record["item_id"].(string)

						if record["type_changed"].(int64) == 1 && record["type_database"] != nil {
							item.Type = record["type_database"].(string)
						}

						if record["nullable_changed"].(int64) == 1 && record["nullable_database"] != nil {
							if record["nullable_database"].(int64) == 1 {
								item.Nullable = true
							} else {
								item.Nullable = false
							}
						}

						if record["example_changed"].(int64) == 1 && record["example_database"] != nil {
							item.Example = record["example_database"].(string)
						}

						if record["default_changed"].(int64) == 1 && record["default_database"] != nil {
							item.Default = record["default_database"].(string)
						}

						if (record["pk_changed"].(int64) == 1 && record["pk_database"] != nil) || (record["keyname_changed"].(int64) == 1 && record["keyname_database"] != nil) {
							k := item.GetKey(record["key_nm"].(string))
							if record["pk_changed"].(int64) == 1 {
								if record["pk_database"].(int64) == 1 {
									k.Type = "primary"
								} else if k.Type == util.EmptyString {
									k.Type = "foreign"
								}
							}
							if record["keyname_changed"].(int64) == 1 {
								k.Name = record["keyname_database"].(string)
							}
							if k != emptykey {
								item.UpsertKey(k)
							}
						}

						d.Update(item)
					}
				} else {
					dsc := record["dh_description"]
					if dsc == util.EmptyString {
						dsc = record["description"]
					}

					item := set.UpsertItem(&doc.Item{
						Id: record["item_id"].(string),
						Name: doc.Name{
							Physical: record["physical_nm"].(string),
							Logical:  record["logical_nm"].(string),
						},
						Comment:  dsc.(string),
						Type:     record["type"].(string),
						Default:  record["default_val"].(string),
						Nullable: record["nullable"].(bool),
						FQDN:     strings.ToLower(set.Name.Physical) + "." + strings.ToLower(record["physical_nm"].(string)),
						Example:  record["example"].(string),
					})

					if record["metadata"] != nil {
						var data map[string]interface{}
						json.Unmarshal([]byte(record["metadata"].(string)), &data)
						item.Metadata = data
					}

					if record["is_pk"].(bool) || (record["key_nm"] != nil && len(record["key_nm"].(string)) > 0) {
						keytype := "foreign"
						if record["is_pk"].(bool) {
							keytype = "primary"
						}

						item.UpsertKey(&doc.Key{
							Name: record["key_nm"].(string),
							Type: keytype,
						})
					}

					d.Update(item)
					// fmt.Println("Update Item: " + err.Error())
					// j, _ := json.MarshalIndent(record, "", "  ")
					// fmt.Println(string(j))
				}
			} else {
				fmt.Printf("WARNING: Failed to identify item parent set (%v) -> %v\n", record["dataset_id"], err.Error())
				j, _ := json.MarshalIndent(record, "", "  ")
				fmt.Println(string(j))
			}
		}

		return nil
	})

	return d, nil
}

func (a *Archive) DiffRelationships(diff *Diff) (*Diff, error) {
	d := CreateDiff()

	rs, err := a.Query(ADD_RELATIONSHIP_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		rel, err := a.getRelationship(record, diff)
		if err == nil {
			d.Add(rel, rel.ID())
		}

		return nil
	})

	rs, err = a.Query(DELETE_RELATIONSHIP_SQL)
	if err != nil {
		fmt.Println(err)
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		rel, err := a.getRelationship(record, diff)
		if err == nil {
			d.Delete(rel, rel.ID())
		}

		return nil
	})

	rs, err = a.Query(UPDATE_RELATIONSHIP_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		rel, err := a.getRelationship(record, diff)
		if err == nil {
			d.Update(rel)
		}

		return nil
	})

	return d, nil
}

func (a *Archive) DiffJoins(setdiff *Diff, diff *Diff) (*Diff, error) {
	d := CreateDiff()

	rs, err := a.Query(ADD_JOIN_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		pparts := strings.Split(record["parent_fqdn"].(string), ".")
		cparts := strings.Split(record["child_fqdn"].(string), ".")

		rel, err := a.getRelationship(record, setdiff)
		if err == nil {
			if !diff.HasDeletion(rel.Set.ID()) {
				cardinality := "1,1,0,-1"
				if record["cardinality"] != util.EmptyString && len(strings.TrimSpace(record["cardinality"].(string))) > 0 {
					cardinality = record["cardinality"].(string)
				}

				join := &doc.Join{
					Parent: &doc.RelItem{
						Schema: pparts[0],
						Set:    pparts[1],
						Item:   pparts[2],
						FQDN:   record["parent_fqdn"].(string),
					},
					Child: &doc.RelItem{
						Schema: cparts[0],
						Set:    cparts[1],
						Item:   cparts[2],
						FQDN:   record["child_fqdn"].(string),
					},
					Position:     int(record["position"].(int64)),
					Cardinality:  cardinality,
					Relationship: rel,
				}

				rel.UpsertJoin(join)

				d.Add(join, join.ID())
			} else {
				// if record["parent_fqdn"] != nil {
				// 	set, err2 := a.getSet(map[string]interface{}{
				// 		"schema":      pparts[0],
				// 		"physical_nm": pparts[1],
				// 	})

				// 	if err2 == nil {
				// 		if diff.HasDeletion(set.ID()) {
				// 			return nil
				// 		}

				// 		set, err2 = a.getSet(map[string]interface{}{
				// 			"schema":      cparts[0],
				// 			"physical_nm": cparts[1],
				// 		})

				// 		if err2 == nil {
				// 			if diff.HasDeletion(set.ID()) {
				// 				return nil
				// 			}

				// 			// set.UpsertRelationship(&doc.Relationship{
				// 			// 	Id: record["db_relationship_id"].(string),
				// 			// 	Name: doc.Name{
				// 			// 		Physical: record["db_relationship_id"].(string),
				// 			// 	},
				// 			// 	Integrity
				// 			// })
				// 		} else {
				// 			return nil
				// 		}
				// 	} else {
				// 		return nil
				// 	}
				// }

				fmt.Println(err)
				j, _ := json.MarshalIndent(record, "", "  ")
				fmt.Println(string(j))
			}
		}

		return nil
	})

	rs, err = a.Query(DELETE_JOIN_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		rel, err := a.getRelationship(record, setdiff)
		if err == nil {
			if !diff.HasDeletion(rel.ID()) {
				pparts := strings.Split(record["parent_fqdn"].(string), ".")
				cparts := strings.Split(record["child_fqdn"].(string), ".")
				cardinality := "1,1,0,-1"

				if record["cardinality"] != util.EmptyString && len(strings.TrimSpace(record["cardinality"].(string))) > 0 {
					cardinality = record["cardinality"].(string)
				}

				join := &doc.Join{
					Parent: &doc.RelItem{
						Schema: pparts[0],
						Set:    pparts[1],
						Item:   pparts[2],
						FQDN:   record["parent_fqdn"].(string),
					},
					Child: &doc.RelItem{
						Schema: cparts[0],
						Set:    cparts[1],
						Item:   cparts[2],
						FQDN:   record["child_fqdn"].(string),
					},
					Position:     int(record["position"].(int64)),
					Cardinality:  cardinality,
					Relationship: rel,
				}

				rel.UpsertJoin(join)

				d.Delete(join, join.ID())
			}
		} else {
			fmt.Println("Error deleting join:", err)
			util.Dump(record)
		}

		return nil
	})

	rs, err = a.Query(UPDATE_JOIN_SQL)
	if err != nil {
		return d, err
	}

	rs.ForEach(func(record map[string]interface{}) error {
		pparts := strings.Split(record["parent_fqdn"].(string), ".")
		cparts := strings.Split(record["child_fqdn"].(string), ".")
		cardinality := "1,1,0,-1"

		if record["cardinality"] != util.EmptyString && len(strings.TrimSpace(record["cardinality"].(string))) > 0 {
			cardinality = record["cardinality"].(string)
		}

		join := &doc.Join{
			Parent: &doc.RelItem{
				Schema: pparts[0],
				Set:    pparts[1],
				Item:   pparts[2],
				FQDN:   record["parent_fqdn"].(string),
			},
			Child: &doc.RelItem{
				Schema: cparts[0],
				Set:    cparts[1],
				Item:   cparts[2],
				FQDN:   record["child_fqdn"].(string),
			},
			Position:    int(record["position"].(int64)),
			Cardinality: cardinality,
		}

		rel, err := a.getRelationship(record, setdiff)
		if err == nil {
			if !diff.HasDeletion(rel.ID()) {
				join.Relationship = rel
				join = rel.UpsertJoin(join)

				d.Update(join, join.ID())
			}
		} else {
			source, err := a.doc.GetSchema(pparts[0])
			if err == nil {
				rel = source.UpsertRelationship(&doc.Relationship{
					Id:   record["relationship_id"].(string),
					Name: doc.Name{Physical: record["db_relationship_id"].(string)},
				})

				join.Relationship = rel

				rel.UpsertJoin(join)
				d.Update(join, join.ID())

				return nil
			}

			fmt.Println("error updating join:", err)
			util.Dump(record)
		}

		return nil
	})

	return d, nil
}

func (a *Archive) getSet(record map[string]interface{}) (*doc.Set, error) {
	var name string
	if nm, exists := record["dataset_id"]; exists {
		name = nm.(string)
	} else if nm, exists = record["physical_nm"]; exists {
		name = record["physical_nm"].(string)
	} else {
		return &doc.Set{}, errors.New("set not found")
	}

	if record["schema"] == nil {
		util.Dump(record)
	}

	schema, err := a.doc.GetSchema(record["schema"].(string))
	if err != nil {
		return &doc.Set{}, err
	}

	return schema.GetSet(name)
}

func (a *Archive) getRelationship(record map[string]interface{}, diff *Diff) (*doc.Relationship, error) {
	if relid, exists := record["db_relationship_id"]; exists {
		if parent, exists := record["parent_fqdn"]; exists {
			parts := strings.Split(parent.(string), ".")

			schema, err := a.doc.GetSchema(parts[0])
			if err != nil {
				return &doc.Relationship{}, err
			}

			if rel, exists := schema.Relationships[relid.(string)]; exists {
				if diff.HasDeletion(rel.Set.ID()) {
					return rel, errors.New("relationship exists, but parent set is scheduled for deletion")
				}
				return rel, nil
			}

			return &doc.Relationship{}, errors.New(relid.(string) + " relationship does not exist or cannot be found in " + parts[0] + " schema")
		}
	}

	set, err := a.getSet(record)
	if err != nil {
		fmt.Println(err)
		util.Dump(record)

		return &doc.Relationship{}, err
	}

	if diff.HasDeletion(set.ID()) {
		return &doc.Relationship{}, errors.New("relationship set is scheduled for deletion")
	}

	rel := set.UpsertRelationship(&doc.Relationship{
		Name: doc.Name{
			Physical: record["physical_nm"].(string),
		},
		Integrity: &doc.ReferentialIntegrity{
			Update: record["on_update"].(string),
			Delete: record["on_delete"].(string),
			Match:  record["on_match"].(string),
		},
	})

	if record["type"] != nil {
		rel.Type = record["type"].(string)
	}

	if record["logical_nm"] != nil {
		rel.Name.Logical = record["logical_nm"].(string)
	}

	if record["id"] != nil {
		rel.Id = record["id"].(string)
	}

	if record["comment"] != nil {
		rel.Comment = record["comment"].(string)
	}

	return rel, nil
}

func (a *Archive) LookupDatahubSet(name string) (*RecordSet, error) {
	return a.Query(`
		SELECT *
		FROM dh_dataset
		WHERE lower(trim(physical_nm)) = '` + strings.ToLower(strings.TrimSpace(name)) + `';
	`)
}
