# Using Jostler for Measurement Service Data

## Introduction

The jostler was designed to accelerate the process of getting new measurement
data into BigQuery.

* measurement service -> JSON -> jostler -> JSONL -> autoloader -> BigQuery

An accelerated publication process places requirements on the output format of
your measurement service. These requirements are easy to satisfy for most
datatypes. And the time you invest in this format will make your data more
accessible and useful to you and others.

### Objective

The jostler is designed to operate similarly to the pusher. The pusher ["API
Contract" and "Best Practices"][pusher] are the same for jostler. Please refer
to this for best practices of where and how to write files.

Since jostler is optimized for JSON datatypes, measurement services deployed
with the jostler uploader agent must:

* Provide a SCHEMA definition for each result datatype (i.e. each BigQuery table)
* Save measurement results as one JSON object per file

The following two sections define a schema file and discuss the JSON result
format.

[pusher]: https://github.com/m-lab/pusher/blob/main/DESIGN.md#4-pushers-api-contract

### JSON Schema for Measurement Data

A JSON schema is a JSON array that contains nested column definitions. Every
column definition must include:

* The column's name, e.g. latitude.
* The column's type, e.g. `FLOAT`, `RECORD`, see all [options for type][tablefieldschema].
* The column mode, i.e. `NULLABLE` or `REPEATED`. Do not use `REQUIRED`.

The column definition may contain:

* The column description.

Column descriptions will appear in BigQuery UI as column documentation. To help
make your data more accessible and usable, description fields are strongly
encouraged.

Other column options are [not currently supported.][tablefieldschema]

[tablefieldschema]: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema

Example JSON Schema:

```json
[
    {
      "description": "geometry record for server locations",
      "fields": [
        {
          "description": "",
          "mode": "NULLABLE",
          "name": "type",
          "type": "STRING"
        },
        {
          "mode": "REPEATED",
          "name": "coordinates",
          "type": "FLOAT"
        }
      ],
      "mode": "NULLABLE",
      "name": "geometry",
      "type": "RECORD"
    },
    {
      "description": "Unique measurement identifier, may be joined with other tables",
      "mode": "NULLABLE",
      "name": "id",
      "type": "STRING"
    }
]
```

### JSON Format for Measurement Data

The jostler only supports JSON result types. For binary or non-JSON formats,
your measurement service should use [pusher](https://github.com/m-lab/pusher).

The JSON result should be for a single measurement and written to a single file;
one measurement, one file (per datatype). If your service collects multiple
datatypes per measurement, save each one in a separate file and datatype
directory, using the [recommended directory structure][jostler-dirs].

[jostler-dirs]: https://github.com/m-lab/jostler#25-default-paths-and-object-names

The outermost JSON type for your measurement result should be an object, i.e.
`{...}`. This object may contain any number of named fields or repeated records.

```json
{
    "id": "1234567",
    "start_time": "2023-03-01T11:34:10Z",
    "samples": [
        {"a": 1},
        {"b": 3},
        {"c": 2}
    ]
}
```

While JSON allows mixed type arrays (e.g. `["a", 1, {}]`), BigQuery does not.
Any array types must use identical repeated types, defined by the result SCHEMA.

Once written to disk, the jostler will read the JSON results and bundle them
together into larger JSONL files. Once enough data has been read or enough time
has passed, the JSONL bundle is uploaded to GCS and the local JSON files that
were part of the bundle are removed.

The content of your measurement data will be wrapped by the jostler ["standard
columns"][stdcolumns]. Your measurement result will always be within the `raw`
record.

[stdcolumns]: https://github.com/m-lab/jostler#221-standard-columns

## Examples

### Using Golang bigquery.InferSchema

If your service is written with Golang, you can use [`bigquery.InferSchema`][1]
with a Go structure to produce usable schema file. For example, the NDT server
includes this logic to produce a JSON schema from the ndt7 structure.

```go
// Generate and save ndt7 schema for autoloading.
row7 := data.NDT7Result{}
sch, err := bigquery.InferSchema(row7)
rtx.Must(err, "failed to generate ndt7 schema")
b, err := sch.ToJSONFields()
rtx.Must(err, "failed to marshal schema")
ioutil.WriteFile(ndt7schema, b, 0o644)
```

[1]: https://pkg.go.dev/cloud.google.com/go/bigquery#InferSchema

### Generating Schema from Sample JSON Object

The `bq` command included in the `google-cloud-sdk` supports creating tables
with schemas inferred from a provided JSON object. to use this method, you need
write access to a GCP BigQuery project. You can setup one for yourself with zero
cost.

The `bq` command can load a sample JSON object from your measurement service
into a new BigQuery table, infer the schema based on the types in the sample
data, and load the data into BigQuery. From that sample data, you can extract
the schema definition to include with your measurement service and for jostler
uploader agent.

[`jq`][jq] is a simple utility for manipulating JSON, available from most Linux
OS package managers.

```sh
bq load --autodetect --source_format NEWLINE_DELIMITED_JSON mlab-sandbox:foo.bar1 ./bar1-sample.json
bq show --format prettyjson mlab-sandbox:foo.bar1 | jq .schema.fields
```

[jq]: https://stedolan.github.io/jq/

### Manually Creating Schema

Since the schema file is JSON, it can be created manually. However, care must be
taken to ensure that no new fields are added to the measurement output without
also being added to the schema file.
