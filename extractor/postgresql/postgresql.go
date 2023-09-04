package postgresql

import (
	"context"
	"dhs/extractor/doc"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	// _ "github.com/lib/pq"
	"github.com/jackc/pgx/v5"
)

type Extractor struct {
	connstring string
	schemas    []string
	conn       *pgx.Conn
	doc        *doc.Doc
}

//go:embed entities.sql
var ENTITY_SQL string

//go:embed relationships.sql
var RELATIONSHIP_SQL string

func New(conn string, schemas []string) Extractor {
	e := Extractor{connstring: conn, schemas: schemas}
	e.SetConnectionString(conn)

	return e
}

func (e Extractor) SQL(statement string, alias ...string) string {
	a := "t"
	if len(alias) > 0 {
		a = string(alias[0])
	}

	filter := a + ".table_schema NOT IN ('pg_catalog', 'information_schema')"

	if len(e.schemas) > 0 {
		filter = ""
		for i, schema := range e.schemas {
			if i > 0 {
				filter = filter + " OR "
			}

			filter = filter + a + ".table_schema LIKE '" + strings.ReplaceAll(schema, "*", "%") + "'"
		}
	}

	return strings.Replace(statement, "[SCHEMA_FILTER]", "("+filter+")", 1)
}

func (e Extractor) SetConnectionString(conn string) error {
	e.connstring = conn

	schema := strings.ToLower(strings.Split(conn, ":")[0])
	if schema != "postgresql" {
		return errors.New("cannot use " + schema + " as a postgresql extractor")
	}

	return nil
}

func (e Extractor) Type() string {
	return "PostgreSQL"
}

func (e Extractor) Extract() (*doc.Doc, error) {
	var empty *doc.Doc

	connstr := strings.Replace(strings.Replace(e.connstring, "postgresql://", "postgres://", 1), "greenplum://", "postgres://", 1)
	uri, err := url.Parse(connstr)
	if err != nil {
		return empty, err
	}

	q := uri.Query()
	if len(strings.TrimSpace(q.Get("sslmode"))) == 0 {
		q.Set("sslmode", "disable")
		uri.RawQuery = q.Encode()
	}

	conn, err := pgx.Connect(context.Background(), uri.String())
	if err != nil {
		return empty, err
	}
	e.conn = conn

	e.doc = doc.New(&doc.Source{
		Name: doc.Name{Physical: strings.Replace(uri.Path, "/", "", 1)},
	})

	err = e.extractEntities()
	if err != nil {
		return empty, err
	}

	err = e.extractRelationships()
	if err != nil {
		return empty, err
	}

	return e.doc, nil
}

func (e Extractor) extractEntities() error {
	return forEachRecord(e.conn, e.SQL(ENTITY_SQL), func(record map[string]interface{}) error {
		return mapEntityToDoc(record, e.doc)
	})
}

func (e Extractor) extractRelationships() error {
	return forEachRecord(e.conn, e.SQL(RELATIONSHIP_SQL, "col"), func(record map[string]interface{}) error {
		return mapRelationshipToDoc(record, e.doc)
	})
}

func forEachRecord(conn *pgx.Conn, sql string, fn func(record map[string]interface{}) error) error {
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get the column names
	columns := make([]string, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		columns[i] = string(fd.Name)
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))

		for i := range values {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			return err
		}

		result := make(map[string]interface{})
		for i, col := range columns {
			result[col] = values[i]
		}

		err := fn(result)
		if err != nil {
			return err
		}
	}

	return rows.Err()
}

func mapEntityToDoc(record map[string]interface{}, document *doc.Doc) error {
	schema := document.ApplySchema(&doc.Schema{
		Name:    doc.Name{Physical: forceString(record["schema"])},
		Comment: forceString(record["schema_comment"]),
		Sets:    make(map[string]*doc.Set),
	})

	set_type := "TABLE"
	if forceBool(record["view"]) {
		set_type = "VIEW"
	} else if forceString(record["entity_type"]) != "base table" {
		set_type = strings.ToUpper(forceString(record["entity_type"]))
	}

	set := schema.UpsertSet(&doc.Set{
		Name:    doc.Name{Physical: forceString(record["entity"])},
		Schema:  schema.Name.Physical,
		Comment: forceString(record["entity_comment"]),
		Type:    set_type,
		Items:   make(map[string]*doc.Item),
	})

	set.UpsertItem(&doc.Item{
		Name:     doc.Name{Physical: forceString(record["name"])},
		Comment:  forceString(record["comment"]),
		Type:     getType(record),
		UDTType:  forceString(record["udt_type"]),
		Identity: forceBool(record["identity"], false),
		Default:  forceString(record["default"], "NULL"),
		Nullable: forceBool(record["nullable"], true),
		FQDN:     forceString(record["fqdn"]),
	})

	return nil
}

func mapRelationshipToDoc(record map[string]interface{}, document *doc.Doc) error {
	// Get set, upsert relationship
	schema := document.ApplySchema(&doc.Schema{
		Name: doc.Name{Physical: forceString(record["source_field_schema"])},
		Sets: make(map[string]*doc.Set),
	})

	set := schema.UpsertSet(&doc.Set{
		Name:   doc.Name{Physical: forceString(record["source_field_schema"])},
		Schema: schema.Name.Physical,
		Items:  make(map[string]*doc.Item),
	})

	rel := set.UpsertRelationship(&doc.Relationship{
		Name:    forceString(record["name"]),
		Type:    forceString(record["type"], "unknown"),
		Comment: forceString(record["comment"]),
		Integrity: &doc.ReferentialIntegrity{
			Update: forceString(record["on_update"]),
			Delete: forceString(record["on_delete"]),
			Match:  forceString(record["on_match"]),
		},
	})

	rel.UpsertJoin(&doc.Join{
		Parent: &doc.RelItem{
			Schema: forceString(record["source_field_schema"]),
			Set:    forceString(record["source_field_entity"]),
			Item:   forceString(record["source_field_name"]),
			FQDN:   forceString(record["source_fqdn"]),
		},
		Child: &doc.RelItem{
			Schema: forceString(record["foreign_field_schema"]),
			Set:    forceString(record["foreign_field_entity"]),
			Item:   forceString(record["foreign_field_name"]),
			FQDN:   forceString(record["foreign_fqdn"]),
		},
		Position:     int(record["key_number"].(int32)),
		Relationship: rel,
	})

	j, _ := json.MarshalIndent(record, "", "  ")
	fmt.Println(string(j))

	j2, _ := json.MarshalIndent(rel, "", "  ")
	fmt.Println(string(j2))

	if false {
		fmt.Println(rel)
		fmt.Println(document)
	}

	return nil
}

func getType(record map[string]interface{}) string {
	t := forceString(record["udt_type"])

	// TODO: Make more specific data types, such as "character varying(250)"
	switch strings.ToLower(t) {
	case "interval":
		t = "interval '" + forceString(record["interval_type"]) + "'"
		if forceString(record["interval_precision"]) != "" {
			t = t + " " + forceString(record["interval_precision"])
		}
	case "float":
		fallthrough
	case "decimal":
		fallthrough
	case "numeric":
	}

	return t
}

func forceString(item interface{}, defaults ...string) string {
	if item == nil {
		if len(defaults) > 0 {
			return defaults[0]
		}

		return ""
	}

	return item.(string)
}

func forceBool(item interface{}, defaults ...bool) bool {
	if item == nil {
		if len(defaults) == 0 {
			return false
		}
		return defaults[0]
	}

	return item.(bool)
}
