// Package main implements jostler.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/m-lab/jostler/internal/uploadbundle"
)

var (
	stdColsFields = map[string]struct{}{
		"Date":                {},
		"Archiver.Version":    {},
		"Archiver.GitCommit":  {},
		"Archiver.ArchiveURL": {},
		"Archiver.Filename":   {},
		"Raw":                 {},
	}
	stdColsTemplate = `[
	  { "mode": "NULLABLE", "name": "date", "type": "DATE" },
	  { 
	    "fields": [
	      { "mode": "NULLABLE", "name": "Version",    "type": "STRING" },
	      { "mode": "NULLABLE", "name": "GitCommit",  "type": "STRING" },
	      { "mode": "NULLABLE", "name": "ArchiveURL", "type": "STRING" },
	      { "mode": "NULLABLE", "name": "Filename",   "type": "STRING" }
	    ],
	    "mode": "NULLABLE", "name": "archiver", "type": "RECORD"
	  },
	  RAW_SCHEMA
	]`

	datatypePathTemplate = "/var/spool/datatypes/DATATYPE/schema.json"

	errNotInStdCols = errors.New("is not in standard columns schema template")
	errFieldType    = errors.New("has unexpected field type")
	errNoBracket    = errors.New("invalid schema: no closing bracket")
	errWriteFile    = errors.New("failed to write file")
	errReadFile     = errors.New("failed to read file")
	errUnmarshal    = errors.New("invalid schema: failed to unmarshal")
)

// validateStdColsTemplate verifies the standard columns schema template
// matches the standard columns we wrap the measurement data in.
func validateStdColsTemplate() error {
	allFields := ""
	stdCols := uploadbundle.StandardColumns{} //nolint:exhaustruct
	if err := allFieldNames(stdCols, "", &allFields); err != nil {
		return err
	}
	for _, s := range strings.Split(allFields, " ") {
		if s != "" {
			if _, ok := stdColsFields[s]; !ok {
				return fmt.Errorf("%v: %w", s, errNotInStdCols)
			}
		}
	}
	return nil
}

// allFieldNames adds the names of all fields in the given struct st to
// the given argument fields.  The given struct can have nested structs
// but all fields are expected to be string.
func allFieldNames(st interface{}, prefix string, fields *string) error {
	val := reflect.ValueOf(st)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	for i := 0; i < val.NumField(); i++ {
		fieldName := val.Type().Field(i).Name
		if prefix != "" {
			fieldName = prefix + "." + fieldName
		}
		switch val.Field(i).Kind() { //nolint:exhaustive
		case reflect.Struct:
			if err := allFieldNames(val.Field(i).Interface(), fieldName, fields); err != nil {
				return err
			}
		case reflect.String:
			*fields = fmt.Sprintf("%v%v ", *fields, fieldName)
		default:
			return fmt.Errorf("%v: %w", fieldName, errFieldType)
		}
	}
	return nil
}

// createTableSchemas creates schema files for each datatype.
// These schema files will be used to create BigQuery tables.
func createTableSchemas() error {
	for _, datatype := range datatypes {
		datatypeSchema, err := schemaForDatatype(datatype)
		if err != nil {
			return err
		}
		// Replace the placeholder "RAW_SCHEMA" with the actual
		// schema for the datatype and write out the table schema.
		s := `, "mode": "NULLABLE", "name": "raw", "type": "RECORD" }`
		i := strings.LastIndex(datatypeSchema, "}")
		if i == -1 {
			return errNoBracket
		}
		tableSchema := strings.Replace(stdColsTemplate, "RAW_SCHEMA", datatypeSchema[:i]+s, 1)
		// XXX Add a flag to optionally specify where to write.
		if err := os.WriteFile(datatype+"-schema.json", []byte(tableSchema), 0o666); err != nil {
			return fmt.Errorf("%v: %w", errWriteFile, err)
		}
		log.Printf("created table schema %s\n", datatype+"-schema.json")
		log.Printf("you should upload to %s/%s/datatypes/%s:schema.json\n", *bucket, *gcsHomeDir, datatype)
	}
	return nil
}

// schemaForDatatype validates the schema file for the given datatype
// and returns it as a string.
//
// By default schema files are in /var/spool/datatypes/<datatype>/schema.json
// but they can be specified via the -schema-file flag.  For example,
// for datatype foo1, it can be: foo1:<path>/<to>/<foo1-schema.json>.
func schemaForDatatype(datatype string) (string, error) {
	// See if a schema file for this datatype was specified on the
	// command line.
	schemaFile := ""
	for i := range schemaFiles {
		if strings.HasPrefix(schemaFiles[i], datatype+":") {
			schemaFile = (schemaFiles[i])[len(datatype)+1:]
			break
		}
	}
	if schemaFile == "" {
		schemaFile = strings.Replace(datatypePathTemplate, "DATATYPE", datatype, 1)
	}
	vLogf("checking schema file %v for datatype %v", schemaFile, datatype)
	contents, err := os.ReadFile(schemaFile)
	if err != nil {
		return "", fmt.Errorf("%v: %w", errReadFile, err)
	}
	// Unmarshal the schema file to validate its well-formed JSON.
	rawSchema := struct{ field map[string]interface{} }{}
	if err = json.Unmarshal(contents, &rawSchema.field); err != nil {
		return "", fmt.Errorf("%v: %v: %w", datatype, errUnmarshal, err)
	}
	return strings.TrimSuffix(string(contents), "\n"), nil
}
