// Package schema implements code that handles datatype and table schemas.
package schema

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
)

// DownloaderUploader interface.
type DownloaderUploader interface {
	Download(context.Context, string) ([]byte, error)
	Upload(context.Context, string, []byte) error
}

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
	LocalDataDir          = "/var/spool"
	GCSHomeDir            = "autoload/v1"
	dtSchemaPathTemplate  = "/datatypes/<datatype>.json"
	tblSchemaPathTemplate = "/tables/<experiment>/<datatype>.table.json"

	ErrStorageClient  = errors.New("failed to create storage client")
	ErrReadSchema     = errors.New("failed to read schema file")
	ErrEmptySchema    = errors.New("empty schema file")
	ErrSchemaFromJSON = errors.New("failed to create schema from JSON")
	ErrMarshal        = errors.New("failed to marshal schema")
	ErrUnmarshal      = errors.New("failed to unmarshal schema")
	ErrCompare        = errors.New("failed to compare schema")
	ErrOnlyInOld      = errors.New("field(s) only in old schema")
	ErrTypeMismatch   = errors.New("difference(s) in schema field types")
	ErrType           = errors.New("unexpected type")
	ErrDownload       = errors.New("failed to download schema")
	ErrUpload         = errors.New("failed to upload schema")

	// Testing and debugging support.
	verbosef = func(fmt string, args ...interface{}) {}
)

// Verbose prints verbosef messages if initialized by the caller.
func Verbose(v func(string, ...interface{})) {
	verbosef = v
}

// PathForDatatype returns the path of the schema file for the given
// datatype.  If the path was explicitly specified on the command line,
// it is used.  Otherwise the default location is assumed.
func PathForDatatype(datatype string, dtSchemaFiles []string) string {
	for i := range dtSchemaFiles {
		if strings.HasPrefix(dtSchemaFiles[i], datatype+":") {
			return (dtSchemaFiles[i])[len(datatype)+1:]
		}
	}
	return LocalDataDir + strings.Replace(dtSchemaPathTemplate, "<datatype>", datatype, 1)
}

// ValidateSchemaFile validates the specified schema file exists and is
// well-formed JSON.
//
// This function can validate any schema file but is typically called
// to validate a datatype schema file.
func ValidateSchemaFile(dtSchemaFile string) error {
	// Does it exist?
	contents, err := os.ReadFile(dtSchemaFile)
	if err != nil {
		return fmt.Errorf("%v: %w", ErrReadSchema, err)
	}
	// Is it well-formed JSON?
	var data []interface{}
	if err = json.Unmarshal(contents, &data); err != nil {
		return fmt.Errorf("%v: %v: %w", dtSchemaFile, ErrUnmarshal, err)
	}
	return nil
}

// CreateTableSchemaJSON creates new table schemas for the given datatype.
func CreateTableSchemaJSON(datatype, dtSchemaFile string) ([]byte, error) {
	tblSchema, err := createTable(datatype, dtSchemaFile)
	if err != nil {
		return nil, err
	}
	tblSchemaJSON, err := tblSchema.ToJSONFields()
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrMarshal, err)
	}
	return tblSchemaJSON, nil
}

// ValidateAndUpload compares the current table schema against the
// previous table schema for the given datatype and returns an error
// if they are not compatibale.  If the new table schema is a superset
// of the previous one, it will be uploaded to GCS.
func ValidateAndUpload(gcsClient DownloaderUploader, bucket, experiment, datatype, dtSchemaFile string) error {
	if err := ValidateSchemaFile(dtSchemaFile); err != nil {
		return err
	}
	diff, err := diffTableSchemas(gcsClient, bucket, experiment, datatype, dtSchemaFile)
	if err != nil {
		if !errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("%v: %w", ErrCompare, err)
		}
		// Scenario 1: old doesn't exist, should upload new.
		verbosef("no old table schema")
		return uploadTableSchema(gcsClient, bucket, experiment, datatype, dtSchemaFile)
	}
	if diff.nInOld != 0 {
		// Scenario 4 - new incompatible with old due to missing fields, should not upload.
		return fmt.Errorf("incompatible schema: %2d %w", diff.nInOld, ErrOnlyInOld)
	}
	if diff.nType != 0 {
		// Scenario 4 - new incompatible with old due to field type mismatch, should not upload.
		return fmt.Errorf("incompatible schema: %2d %w", diff.nType, ErrTypeMismatch)
	}
	if diff.nInNew != 0 {
		// Scenario 3 - new is a superset of old, should upload.
		verbosef("%2d field(s) only in new schema", diff.nInNew)
		return uploadTableSchema(gcsClient, bucket, experiment, datatype, dtSchemaFile)
	}
	// Scenario 2 - old exists and matches new, should not upload.
	return nil
}

// diffTableSchemas builds a new table schema for the given datatype,
// compares it against the old table schema (if it exists), and returns
// their differences.
func diffTableSchemas(gcsClient DownloaderUploader, bucket, experiment, datatype, dtSchemaFile string) (*mapDiff, error) {
	// Fetch the old table schema if it exists.  If it doesn't exist,
	// there is nothing to validate for this datatype and the new table
	// schema should be uploaded.
	ctx := context.Background()
	objPath := tblSchemaPath(experiment, datatype)
	// Create a storage client for downloading.
	verbosef("downloading '%v:%v'", bucket, objPath)
	oldTblSchemaJSON, err := gcsClient.Download(ctx, objPath)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrDownload, err)
	}
	verbosef("successfully downloaded '%v:%v'", bucket, objPath)
	// We need a better way of handling the following changes.
	s := string(oldTblSchemaJSON)
	s = strings.ReplaceAll(s, `"name":`, `"Name":`)
	s = strings.ReplaceAll(s, `"type":`, `"Type":`)
	oldTblSchemaJSON = []byte(s)

	// Old table schema exists.
	oldFieldsMap, err := allFields(oldTblSchemaJSON)
	if err != nil {
		return nil, err
	}
	if len(oldFieldsMap) == 0 {
		return nil, fmt.Errorf("%v: %w", ErrEmptySchema, err)
	}

	// Create the new table schema and marshal it to a JSON object
	// to compare with the old one.
	newTblSchema, err := createTable(datatype, dtSchemaFile)
	if err != nil {
		return nil, err
	}
	newTblSchemaJSON, err := json.Marshal(newTblSchema)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrMarshal, err)
	}
	newFieldsMap, err := allFields(newTblSchemaJSON)
	if err != nil {
		return nil, err
	}

	// Compare the old field with the new field.  Deleting old fields
	// or changing their types is a breaking change.
	return compareMaps(oldFieldsMap, newFieldsMap), nil
}

// tblSchemPath returns the GCS object name (aka path) for the given
// experiment and datatype.
func tblSchemaPath(experiment, datatype string) string {
	objPath := strings.Replace(tblSchemaPathTemplate, "<experiment>", experiment, 1)
	return GCSHomeDir + strings.Replace(objPath, "<datatype>", datatype, 1)
}

// uploadTableSchema creates a table schema for the given datatype schema
// and uploads it to GCS.
func uploadTableSchema(gcsClient DownloaderUploader, bucket, experiment, datatype, dtSchemaFile string) error {
	ctx := context.Background()
	tblSchemaJSON, err := CreateTableSchemaJSON(datatype, dtSchemaFile)
	if err != nil {
		return err
	}
	objPath := tblSchemaPath(experiment, datatype)
	// Create a storage client for uploading.
	verbosef("uploading '%v:%v'", bucket, objPath)
	if err := gcsClient.Upload(ctx, objPath, tblSchemaJSON); err != nil {
		return fmt.Errorf("%v: %w", ErrUpload, err)
	}
	verbosef("successfully uploaded '%v:%v'", bucket, objPath)
	return nil
}

// createTable creates a new table schema with the standard columns for
// the given datatype and returns it.
func createTable(datatype, dtSchemaFile string) (bigquery.Schema, error) {
	dtSchema, err := fromJSON(dtSchemaFile)
	if err != nil {
		return nil, err
	}
	stdColsSchema, err := bigquery.InferSchema(api.StandardColumnsV0{})
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema for %v: %w", datatype, err)
	}
	return replaceField("raw", stdColsSchema, dtSchema), nil
}

// allFields returns a map of all fields in the given schema.  The key
// of each map entry is the full field name and its value is the field
// type (e.g., ["archiver.Version"]: "STRING").
func allFields(schemaJSON []byte) (map[string]string, error) {
	var schema []interface{}
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return nil, fmt.Errorf("%v: %w", ErrUnmarshal, err)
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
			return fmt.Errorf("%T: %w", field, ErrType)
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
				return fmt.Errorf("%T: %w", f["fields"], ErrType)
			}
		}
		if err := visitAllFieldsRecursive(record, visit, ffn); err != nil {
			return err
		}
	}
	return nil
}

// fromJSON returns a BigQuery schema from the specified file which is
// expected to be in JSON format.
func fromJSON(dtSchemaFile string) (bigquery.Schema, error) {
	contents, err := os.ReadFile(dtSchemaFile)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrReadSchema, err)
	}
	schema, err := bigquery.SchemaFromJSON(contents)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrSchemaFromJSON, err)
	}
	return schema, nil
}

// replaceField replaces the specified field in the original schema
// by the replacing schema.
func replaceField(fieldName string, origSchema, replacingSchema bigquery.Schema) bigquery.Schema {
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

// compareMaps compares the given maps and returns their differences
// as three integers that are the number of (1) keys only in the new map,
// (2) keys only in the old map, and (3) different values.  It also logs
// the comparison results in verbosef mode.
func compareMaps(oldMap, newMap map[string]string) *mapDiff {
	diff := &mapDiff{}
	newKeys := sortMapKeys(newMap)
	for _, n := range newKeys {
		if _, ok := oldMap[n]; !ok {
			verbosef("%-10s %v:%v", "only in new:", n, newMap[n])
			diff.nInNew++
			continue
		}
		// The key exists in both schemas; compare their values.
		if newMap[n] != oldMap[n] {
			verbosef("%-10v %v:%v in new, %v:%v in old", "mismatch:", n, newMap[n], n, oldMap[n])
			diff.nType++
			continue
		}
		verbosef("%-10s %v:%v", "in both:", n, newMap[n])
	}
	oldKeys := sortMapKeys(oldMap)
	for _, o := range oldKeys {
		if _, ok := newMap[o]; !ok {
			verbosef("%-10s %v:%v", "only in old:", o, oldMap[o])
			diff.nInOld++
		}
	}
	return diff
}

// sortMapKeys returns a sorted slice of all keys in the given map.
func sortMapKeys(fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
