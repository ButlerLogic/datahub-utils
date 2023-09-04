package postgresql

import "database/sql"

type EntityInfo struct {
	Schema             sql.NullString `json:"schema"`
	SchemaComment      sql.NullString `json:"schame_comment"`
	Entity             sql.NullString `json:"entity"`
	EntityFQDN         sql.NullString `json:"entity_fqdn"`
	Name               sql.NullString `json:"name"`
	FQDN               sql.NullString `json:"fqdn"`
	Position           sql.NullInt64  `json:"position"`
	Type               sql.NullString `json:"type"`
	Nullable           sql.NullBool   `json:"nullable"`
	Default            sql.NullString `json:"default"`
	MaxLength          sql.NullInt64  `json:"max_length"`
	NumericPrecision   sql.NullInt64  `json:"numeric_precision"`
	NumericRadix       sql.NullInt64  `json:"numeric_radix"`
	NumericScale       sql.NullInt64  `json:"numeric_scale"`
	IdentityCycle      sql.NullBool   `json:"identity_cycle"`
	IdentityIncrement  sql.NullInt64  `json:"identity_increment"`
	IdentityGeneration sql.NullString `json:"identity_generation"`
	IdentityStart      sql.NullInt64  `json:"identity_start"`
	Identity           sql.NullBool   `json:"identity"`
	IdentityMinimum    sql.NullInt64  `json:"identity_min"`
	IdentityMaximum    sql.NullInt64  `json:"identity_max"`
	UDTType            sql.NullString `json:"udt_type"`
	DateTimePrecision  sql.NullInt64  `json:"datetime_precision"`
	IntervalType       sql.NullString `json:"interval_type"`
	IntervalPrecision  sql.NullInt64  `json:"interval_precision"`
	EntityType         sql.NullString `json:"entity_type"`
	IsView             sql.NullBool   `json:"is_view"`
	PrimaryKey         sql.NullBool   `json:"is_primary_key"`
	EntityComment      sql.NullString `json:"entity_comment"`
	Comment            sql.NullString `json:"comment"`
	IsKey              sql.NullBool   `json:"is_key"`
	KeyPosition        sql.NullInt64  `json:"key_position"`
	KeyType            sql.NullString `json:"key_type"`
	KeyName            sql.NullString `json:"key_name"`
}
