// Package main implements jostler.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/m-lab/jostler/api"
	"github.com/m-lab/jostler/internal/gcs"
)

type (
	bqField   map[string]interface{}
	visitFunc func([]string, bqField) error
	mapDiff   struct {
		nInOld int
		nInNew int
		nType  int
	}
)

var (
	datatypePathTemplate = "/var/spool/datatypes/{{DATATYPE}}/schema.json"
	objectPrefixTemplate = "autoload/v0/datatypes/{{EXPERIMENT}}"

	errReadSchema     = errors.New("failed to read schema file")
	errSchemaFromJSON = errors.New("failed to create schema from JSON")
	errMarshal        = errors.New("failed to marshal schema")
	errUnmarshal      = errors.New("failed to unmarshal schema")
	errCompare        = errors.New("failed to compare schema")
	errOnlyInOld      = errors.New("field(s) only in old schema")
	errTypeMismatch   = errors.New("difference(s) in schema field types")
	errType           = errors.New("unexpected type")
	errDownload       = errors.New("failed to upload schema")
	errUpload         = errors.New("failed to download schema")
)

// createNewTableSchemaJSON creates new table schemas for all datatypes
// and optionally uploads them to GCS.
func createNewTableSchemaJSON(datatype string) ([]byte, error) {
	newTableSchema, err := createNewTableSchema(datatype)
	if err != nil {
		return nil, err
	}
	schemaJSON, err := newTableSchema.ToJSONFields()
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errMarshal, err)
	}
	return schemaJSON, nil
}

// uploadTableSchemas compares the current table schemas against the previous
// table schemas for all datatypes and uploads to GCS if needed.
func uploadTableSchemas(bucket, experiment string, datatypes []string) error {
	for _, datatype := range datatypes {
		diff, err := diffTableSchema(bucket, experiment, datatype)
		if err != nil {
			return fmt.Errorf("%v: %w", errCompare, err)
		}
		// New schema is not backward compatible with old.
		if diff.nInOld != 0 {
			return fmt.Errorf("incompatible schema: %2d %w", diff.nInOld, errOnlyInOld)
		}
		// New schema is not backward compatible with old.
		if diff.nType != 0 {
			return fmt.Errorf("incompatible schema: %2d %w", diff.nType, errTypeMismatch)
		}
		// New schema has additional fields but is backward compatible with old.
		if diff.nInNew != 0 {
			vLogf("%2d field(s) only in new schema", diff.nInNew)
			ctx := context.Background()
			prefix := strings.Replace(objectPrefixTemplate, "{{EXPERIMENT}}", experiment, 1)
			objPath := fmt.Sprintf("%s/%s-schema.json", prefix, datatype)
			schemaJSON, err := createNewTableSchemaJSON(datatype)
			if err != nil {
				return err
			}
			if err := gcs.Upload(ctx, bucket, objPath, schemaJSON); err != nil {
				return fmt.Errorf("%v: %w", errUpload, err)
			}
		}
	}
	return nil
}

// diffTableSchema builds a new table schema for the given datatype,
// compares it against the old table schema (if it exists), and returns
// their differences.
func diffTableSchema(bucket, experiment, datatype string) (*mapDiff, error) {
	// Fetch the old table schema if it exists.  If it doesn't exist,
	// there is nothing to validate for this datatype and the new table
	// schema should be uploaded.
	ctx := context.Background()
	prefix := strings.Replace(objectPrefixTemplate, "{{EXPERIMENT}}", experiment, 1)
	objPath := fmt.Sprintf("%s/%s-schema.json", prefix, datatype)
	oldTableSchemaJSON, err := gcs.Download(ctx, bucket, objPath)
	if err != nil {
		if !errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("%v: %w", errDownload, err)
		}
		vLogf("'%v:%v' does not exist", bucket, objPath)
		// Set nInNew to a non-zero value so the new table schema
		// will be uploaded to GCS.
		return &mapDiff{nInNew: 1}, nil
	}
	// We need a better way of handling the following changes.
	s := string(oldTableSchemaJSON)
	s = strings.ReplaceAll(s, `"name":`, `"Name":`)
	s = strings.ReplaceAll(s, `"type":`, `"Type":`)
	oldTableSchemaJSON = []byte(s)

	// Old table schema exists.
	oldFieldsMap, err := schemaFields(oldTableSchemaJSON)
	if err != nil {
		return nil, err
	}
	if len(oldFieldsMap) == 0 {
		panic("empty old schema field")
	}

	// Create the new table schema and marshal it to a JSON object
	// to compare with the old one.
	newTableSchema, err := createNewTableSchema(datatype)
	if err != nil {
		return nil, err
	}
	newTableSchemaJSON, err := json.Marshal(newTableSchema)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errMarshal, err)
	}
	newFieldsMap, err := schemaFields(newTableSchemaJSON)
	if err != nil {
		return nil, err
	}

	// Compare the old field with the new field.  Deleting old fields
	// or changing their types is a breaking change.
	return compareMaps(oldFieldsMap, newFieldsMap), nil
}

// createNewTableSchema creates a new table schema with the standard
// columns for the given datatype and returns it.
func createNewTableSchema(datatype string) (bigquery.Schema, error) {
	datatypeSchema, err := schemaFromJSON(datatype)
	if err != nil {
		return nil, err
	}
	stdColsSchema, err := bigquery.InferSchema(api.StandardColumnsV0{})
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema for %v: %w", datatype, err)
	}
	return replaceSchemaField("raw", stdColsSchema, datatypeSchema), nil
}

// schemaFields returns a map of all fields in the given schema.  The key
// of each map entry is the full field name and its value is the field
// type (e.g., ["archiver.Version"]: "STRING").
func schemaFields(schemaJSON []byte) (map[string]string, error) {
	var schema []interface{}
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return nil, fmt.Errorf("%v: %w", errUnmarshal, err)
	}
	fields := make(map[string]string)
	err := visitAllFields(schema, func(fullFieldName []string, field bqField) error {
		if key := strings.Join(fullFieldName, "."); key != "" {
			fields[key] = fmt.Sprintf("%v", field["Type"])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fields, nil
}

// visitAllFields calls the given visit function for each field in the
// given schema.
func visitAllFields(schema []interface{}, visit visitFunc) error {
	return visitAllFieldsRecursive(schema, visit, []string{})
}

// visitAllFieldsRecursive visits all fields in the given schema, calling
// itself recursively for RECORD field types.
func visitAllFieldsRecursive(schema []interface{}, visit visitFunc, fullFieldName []string) error {
	for _, field := range schema {
		var f bqField
		f, ok := field.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%T: %w", field, errType)
		}
		ffn := fullFieldName
		ffn = append(ffn, fmt.Sprintf("%v", f["Name"]))
		if err := visit(ffn, f); err != nil {
			return err
		}
		if f["Type"] != "RECORD" {
			continue
		}
		record, ok := f["Schema"].([]interface{})
		if !ok {
			record, ok = f["fields"].([]interface{})
			if !ok {
				return fmt.Errorf("%T: %w", f["fields"], errType)
			}
		}
		if err := visitAllFieldsRecursive(record, visit, ffn); err != nil {
			return err
		}
	}
	return nil
}

// schemaFromJSON returns a BigQuery schema from the specified file
// which is expected to be in JSON format.
func schemaFromJSON(datatype string) (bigquery.Schema, error) {
	contents, err := os.ReadFile(schemaPathForDatatype(datatype))
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errReadSchema, err)
	}
	schema, err := bigquery.SchemaFromJSON(contents)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errSchemaFromJSON, err)
	}
	return schema, nil
}

// schemaPathForDatatype returns the path of the schema file for the
// given datatypes.  If the path was explicitly specified on the command
// line, it is used.  Otherwise the default location is assumed.
func schemaPathForDatatype(datatype string) string {
	for i := range schemaFiles {
		if strings.HasPrefix(schemaFiles[i], datatype+":") {
			return (schemaFiles[i])[len(datatype)+1:]
		}
	}
	return strings.Replace(datatypePathTemplate, "{{DATATYPE}}", datatype, 1)
}

// replaceSchemaField replaces the specified field in the original schema
// by the replacing schema.
func replaceSchemaField(fieldName string, origSchema, replacingSchema bigquery.Schema) bigquery.Schema {
	schema := bigquery.Schema{}
	for _, fieldSchema := range origSchema {
		if fieldSchema.Name == fieldName {
			rawFieldSchema := &bigquery.FieldSchema{
				Name:     "raw",
				Required: false,
				Type:     bigquery.RecordFieldType,
				Schema:   replacingSchema,
			}
			schema = append(schema, rawFieldSchema)
		} else {
			schema = append(schema, fieldSchema)
		}
	}
	return schema
}

// schemaForDatatype validates the schema file for the given datatype
// and returns it as a string.
//
// By default schema files are in /var/spool/datatypes/<datatype>/schema.json
// but they can be specified via the -schema-file flag.  For example,
// for datatype foo1, it can be: foo1:<path>/<to>/<foo1-schema.json>.
func schemaForDatatype(datatype string) error {
	schemaFile := schemaPathForDatatype(datatype)
	vLogf("checking schema file '%v' for datatype '%v'", schemaFile, datatype)
	contents, err := os.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("%v: %w", errReadSchema, err)
	}
	// Unmarshal the schema file to validate its well-formed JSON.
	var data []interface{}
	if err = json.Unmarshal(contents, &data); err != nil {
		return fmt.Errorf("%v: %v: %w", schemaFile, errUnmarshal, err)
	}
	return nil
}

// compareMaps compares the given maps and returns their differences
// as three integers that are the number of (1) keys only in the new map,
// (2) keys only in the old map, and (3) different values.  It also logs
// the comparison results in verbose mode.
func compareMaps(oldMap, newMap map[string]string) *mapDiff {
	diff := &mapDiff{}
	newKeys := sortedMapKeys(newMap)
	for _, n := range newKeys {
		if _, ok := oldMap[n]; !ok {
			vLogf("%-10s %v:%v", "only in new:", n, newMap[n])
			diff.nInNew++
			continue
		}
		// The key exists in both schemas; compare their values.
		if newMap[n] != oldMap[n] {
			vLogf("%-10v %v:%v in new, %v:%v in old", "mismatch:", n, newMap[n], n, oldMap[n])
			diff.nType++
			continue
		}
		vLogf("%-10s %v:%v", "in both:", n, newMap[n])
	}
	oldKeys := sortedMapKeys(oldMap)
	for _, o := range oldKeys {
		if _, ok := newMap[o]; !ok {
			vLogf("%-10s %v:%v", "only in old:", o, oldMap[o])
			diff.nInOld++
		}
	}
	return diff
}

// sortedMapKeys returns a sorted slice of all keys in the given map.
func sortedMapKeys(fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
