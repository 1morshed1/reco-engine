// Package migrations embeds the SQL schema so any command can apply it without
// depending on a file path at runtime. The .sql file is the single source of
// truth; this just makes it a compiled-in string.
package migrations

import _ "embed"

//go:embed 001_schema.sql
var Schema string
