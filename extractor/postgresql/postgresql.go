package postgresql

import (
	"context"
	"dhs/extractor/doc"
	"dhs/util"
	_ "embed"
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
	Debug      bool
}

//go:embed sql/entities.sql
var ENTITY_SQL string

//go:embed sql/relationships.sql
var RELATIONSHIP_SQL string

//go:embed sql/materialized_views.sql
var MATVIEW_SQL string

//go:embed sql/stats.sql
var STATS_SQL string

//go:embed sql/introspect.sql
var DB_SQL string

func New(conn string, schemas []string) Extractor {
	e := Extractor{connstring: conn, schemas: schemas}
	e.SetConnectionString(conn)
	e.Debug = false

	return e
}

// func (e Extractor) Query(statement string) ([]map[string]interface{}, error) {
// 	result := make([]map[string]interface{}, 0)
// 	err := e.forEachRecord(e.conn, statement, func(record map[string]interface{}) error {
// 		result = append(result, record)
// 		return nil
// 	})

// 	return result, err
// }

func (e Extractor) ExpandJSONFields(d *doc.Doc, skipviews bool, fields ...string) {
	all := false
	if len(fields) == 0 || util.InSlice[string]("*", fields) {
		all = true
	}

	conn, err := e.connect()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close(context.Background())

	for _, items := range d.GetItemsByType("json", "jsonb") {
		for _, item := range items {
			if !skipviews || !strings.Contains(strings.ToUpper(item.Set().Type), "VIEW") {
				if all || util.InSlice[string](strings.ToLower(item.FQDN), fields) {
					table := strings.Join(util.Map[string](strings.Split(item.Set().FQDN, "."), func(el string) string { return `"` + el + `"` }), ".")
					// expandsql := `
					// 	SELECT distinct keys.key as attribute
					// 	FROM ` + table + `
					// 	CROSS JOIN LATERAL (
					// 			SELECT json_object_keys("` + item.Name.Physical + `"::json) AS key
					// 			WHERE json_typeof("` + item.Name.Physical + `"::json) = 'object'
					// 	) AS keys(key);
					// `
					expandsql := `
						WITH RankedRows AS (
							SELECT
									"` + item.Name.Physical + `"
								, ROW_NUMBER() OVER (ORDER BY jsonb_object_keys("` + item.Name.Physical + `"::jsonb) DESC) AS rn
							FROM ` + table + `
							WHERE json_typeof("` + item.Name.Physical + `"::json) = 'object'
							ORDER BY jsonb_object_keys("` + item.Name.Physical + `"::jsonb) DESC
							LIMIT 1
						)
						SELECT DISTINCT keys.key AS attribute
						FROM RankedRows
						CROSS JOIN LATERAL (
								SELECT json_object_keys("` + item.Name.Physical + `"::json) AS key
								WHERE json_typeof("` + item.Name.Physical + `"::json) = 'object'
						) AS keys(key)
						WHERE rn = 1;
					`

					err := forEachRecord(conn, expandsql, func(record map[string]interface{}) error {
						// j, _ := json.MarshalIndent(record, "", "  ")
						jsonItem := &doc.Item{
							Name: doc.Name{Physical: item.Name.Physical + "::" + record["attribute"].(string)},
							Type: "text",
							// UDTType:  "text",
							Identity: false,
							Nullable: true,
							FQDN:     item.Set().FQDN + "." + record["attribute"].(string),
							Metadata: map[string]interface{}{
								"source": item.Name.Physical,
							},
						}

						item.Set().UpsertItem(jsonItem)
						item.UpsertKey(&doc.Key{
							Name:    strings.ToLower(item.Set().Name.Physical) + "_json_expansion_key",
							Type:    "json",
							Comment: "Generated by JSON autoexpansion of the " + item.Set().Name.Physical + " item.",
						})

						return nil
					})

					if err != nil {
						fmt.Println(expandsql)
						fmt.Println(err)
					}
				}
			}
		}
	}
}

func (e Extractor) SQL(statement string, alias ...string) string {
	a := "t.table_schema"
	if len(alias) > 0 {
		a = string(alias[0])
	}

	filter := a + " NOT IN ('pg_catalog', 'information_schema')"

	if len(e.schemas) > 0 {
		filter = ""
		for i, schema := range e.schemas {
			if i > 0 {
				filter = filter + " OR "
			}

			filter = filter + a + " LIKE '" + strings.ReplaceAll(schema, "*", "%") + "'"
		}
	}

	statement = strings.ReplaceAll(statement, "[SCHEMA_FILTER]", "("+filter+")")

	return statement
}

func (e Extractor) SetConnectionString(conn string) error {
	e.connstring = util.EncodeURL(conn)

	schema := strings.ToLower(strings.Split(conn, ":")[0])
	if schema != "postgresql" {
		return errors.New("cannot use " + schema + " as a postgresql extractor")
	}

	return nil
}

func (e Extractor) Type() string {
	return "PostgreSQL"
}

func (e Extractor) connect() (*pgx.Conn, error) {
	if e.Debug {
		fmt.Println("establishing connection...")
	}

	e.connstring = strings.Replace(strings.Replace(e.connstring, "postgresql://", "postgres://", 1), "greenplum://", "postgres://", 1)
	uri, err := url.Parse(e.connstring)
	if err != nil {
		return &pgx.Conn{}, err
	}

	q := uri.Query()
	if len(strings.TrimSpace(q.Get("sslmode"))) == 0 {
		q.Set("sslmode", "prefer")
		uri.RawQuery = q.Encode()
	}

	return pgx.Connect(context.Background(), uri.String())
}

func (e Extractor) Extract() (*doc.Doc, error) {
	if e.Debug {
		fmt.Println("  ... extraction initiated")
	}

	var empty *doc.Doc

	if e.Debug {
		fmt.Println("  ... connecting to database")
	}
	conn, err := e.connect()
	if err != nil {
		return empty, err
	}
	defer conn.Close(context.Background())

	e.conn = conn

	uri, _ := url.Parse(e.connstring)
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

	err = e.extractMaterializedViews()
	if err != nil {
		return empty, err
	}

	err = e.extractStatistics()
	if err != nil {
		return empty, err
	}

	err = e.extractDatabaseDetails()
	if err != nil {
		return empty, err
	}

	if e.Debug {
		fmt.Println("  ... extraction complete")
	}

	return e.doc, nil
}

func (e Extractor) extractDatabaseDetails() error {
	if e.Debug {
		fmt.Println("  ... extracting database details")
	}

	db := e.doc.Source()
	sql := strings.ReplaceAll(DB_SQL, "[DATABASE]", db.Name.Physical)

	return forEachRecord(e.conn, sql, func(record map[string]interface{}) error {
		if record["description"] != nil {
			db.Comment = forceString(record["description"])
		}

		return nil
	})
}

func (e Extractor) extractEntities() error {
	if e.Debug {
		fmt.Println("  ... extracting database entities")
	}
	return forEachRecord(e.conn, e.SQL(ENTITY_SQL), func(record map[string]interface{}) error {
		return mapEntityToDoc(record, e.doc)
	})
}

func (e Extractor) extractRelationships() error {
	if e.Debug {
		fmt.Println("  ... extracting database relationships")
	}
	return forEachRecord(e.conn, e.SQL(RELATIONSHIP_SQL, "col.table_schema"), func(record map[string]interface{}) error {
		return mapRelationshipToDoc(record, e.doc)
	})
}

func (e Extractor) extractMaterializedViews() error {
	if e.Debug {
		fmt.Println("  ... extracting database materialized views")
	}
	return forEachRecord(e.conn, e.SQL(MATVIEW_SQL, "m.schemaname"), func(record map[string]interface{}) error {
		return mapEntityToDoc(record, e.doc)
	})
}

func (e Extractor) extractStatistics() error {
	if e.Debug {
		fmt.Println("  ... extracting database statistics")
	}
	return forEachRecord(e.conn, e.SQL(STATS_SQL, "schemaname"), func(record map[string]interface{}) error {
		return mapEntityStats(record, e.doc)
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

func mapEntityStats(record map[string]interface{}, document *doc.Doc) error {
	schema := document.ApplySchema(&doc.Schema{
		Name: doc.Name{Physical: forceString(record["schema"])},
		Sets: make(map[string]*doc.Set),
	})

	set, err := schema.GetSet(forceString(record["set"]))
	if err == nil {
		item, err := set.GetItem(forceString(record["item"]))
		if err == nil {
			if item.Metadata == nil {
				item.Metadata = map[string]interface{}{}
			}

			item.Metadata["most_common_value"] = forceString(record["most_common_value"], "NULL")
			item.Metadata["null_percentage"] = record["null_fraction"]

			if record["most_common_value"] != nil {
				item.Example = record["most_common_value"].(string)
			}
		}
	}

	return nil
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
		if forceString(record["entity_type"]) == "materialized view" {
			set_type = "MATERIALIZED VIEW"
		}
	} else if forceString(record["entity_type"]) != "base table" {
		set_type = strings.ToUpper(forceString(record["entity_type"]))
	}

	set := schema.UpsertSet(&doc.Set{
		Name:    doc.Name{Physical: forceString(record["entity"]), Logical: forceString(record["entity"])},
		Schema:  schema.Name.Physical,
		Comment: forceString(record["entity_comment"]),
		Type:    set_type,
		Items:   make(map[string]*doc.Item),
	})

	if record["definition"] != nil && len(strings.TrimSpace(record["definition"].(string))) > 0 {
		set.Source = record["definition"].(string)

		if set.Type == util.EmptyString {
			set.Type = "VIEW"
		}
	}

	item := &doc.Item{
		Name:    doc.Name{Physical: forceString(record["name"])},
		Comment: forceString(record["comment"]),
		// Type:     getType(record),
		Type: forceString(record["udt_type"]),
		// UDTType:  forceString(record["udt_type"]),
		Identity: forceBool(record["identity"], false),
		Default:  forceString(record["default"], "NULL"),
		Nullable: forceBool(record["nullable"], true),
		FQDN:     forceString(record["fqdn"]),
	}

	if record["example"] != nil {
		item.Example = forceString(record["example"])
	}

	if forceBool(record["key"], false) {
		key := item.GetKey(forceString(record["key_name"]))
		if key == nil {
			key = &doc.Key{
				Items: make([]string, 0),
			}
			set.UpsertKey(key)
		}

		key.Type = strings.Replace(forceString(record["key_type"], "foreign"), " key", "", 1)
		if forceBool(record["primary_key"], false) {
			key.Type = "primary"
		}

		key.Name = forceString(record["key_name"])
		if !util.InSlice[string](item.FQDN, key.Items) {
			key.Items = append(key.Items, item.FQDN)
		}

		item.UpsertKey(key)
	}

	// util.Dump(record)
	// if record["archive_dt"] != nil {
	// 	util.Dump(record)
	// }

	set.UpsertItem(item)

	return nil
}

func mapRelationshipToDoc(record map[string]interface{}, document *doc.Doc) error {
	// Get set, upsert relationship
	schema := document.ApplySchema(&doc.Schema{
		Name: doc.Name{Physical: forceString(record["source_field_schema"])},
		Sets: make(map[string]*doc.Set),
	})

	set := schema.UpsertSet(&doc.Set{
		Name:  doc.Name{Physical: forceString(record["source_field_schema"])},
		Items: make(map[string]*doc.Item),
	})

	rel := set.UpsertRelationship(&doc.Relationship{
		Name:    doc.Name{Physical: forceString(record["name"])},
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

	// j, _ := json.MarshalIndent(record, "", "  ")
	// fmt.Println(string(j))

	// j2, _ := json.MarshalIndent(rel, "", "  ")
	// fmt.Println(string(j2))

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
