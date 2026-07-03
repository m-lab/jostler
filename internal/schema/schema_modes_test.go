// Package schema_test implements black-box unit testing for package schema.
package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/m-lab/jostler/internal/schema"
)

// TestCreateTableSchemaJSONModes verifies that generated table schemas
// contain no REQUIRED fields.  BigQuery rejects schema updates that change
// a field's mode from NULLABLE to REQUIRED, so a REQUIRED field in the
// uploaded table schema would make it inapplicable to existing tables.
func TestCreateTableSchemaJSONModes(t *testing.T) {
	tblSchemaJSON, err := schema.CreateTableSchemaJSON("foo1", "testdata/datatypes/foo1-valid.json")
	if err != nil {
		t.Fatalf("CreateTableSchemaJSON() error = %v, want nil", err)
	}

	var fields []map[string]interface{}
	if err := json.Unmarshal(tblSchemaJSON, &fields); err != nil {
		t.Fatalf("failed to unmarshal table schema: %v", err)
	}

	var checkModes func(fields []map[string]interface{}, prefix string)
	checkModes = func(fields []map[string]interface{}, prefix string) {
		for _, field := range fields {
			name := prefix + field["name"].(string)
			if field["mode"] == "REQUIRED" {
				t.Errorf("field %q has mode REQUIRED, want NULLABLE", name)
			}
			if subFields, ok := field["fields"].([]interface{}); ok {
				sub := make([]map[string]interface{}, 0, len(subFields))
				for _, s := range subFields {
					sub = append(sub, s.(map[string]interface{}))
				}
				checkModes(sub, name+".")
			}
		}
	}
	checkModes(fields, "")
}
