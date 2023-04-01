# Using Jostler for Measurement Service Data

## Introduction

The jostler was designed to accelerate the process of getting new measurement
data into BigQuery.

* measurement service -> JSON -> jostler -> JSONL -> autoloader -> BigQuery

An accelerated publication process places requirements on the output format of
your measurement service. These requirements are easy to satisfy for most
datatypes. And the time you invest in this format will make your data more
accessible and useful to you and others.

### Overview

Since jostler is optimized for JSON datatypes, measurement services deployed
with the jostler uploader agent must:

* Save measurement results as one JSON object per file
* Provide a SCHEMA definition for each result datatype (i.e. each BigQuery table)

The following two sections discuss the JSON result format and define a schema
file for a sample result type.

The jostler is designed to operate similarly to the pusher. The pusher ["API
Contract" and "Best Practices"][pusher] are the same for jostler. Please refer
to this for best practices of where and how to write files.

[pusher]: https://github.com/m-lab/pusher/blob/main/DESIGN.md#4-pushers-api-contract

### JSON Format for Measurement Data

The jostler only supports JSON result types. For binary or non-JSON formats,
your measurement service should use [pusher](https://github.com/m-lab/pusher).

The JSON result should be for a single measurement and written to a single file;
one measurement, one file (per datatype). If your service collects multiple
datatypes per measurement, save each one in a separate file and datatype
directory, using the [recommended directory structure][jostler-dirs].

[jostler-dirs]: https://github.com/m-lab/jostler#25-default-paths-and-object-names

The outermost JSON type for your measurement result should be an object, i.e.
`{...}`. The object should be formatted as a single line (not pretty printed).
This object may contain any number of named fields or repeated records.

NOTE: the example is pretty printed for clarity, but an actual file should not be.

```json
{
    "UUID": "abcdefg-1234567",
    "Server": "192.168.0.1",
    "Client": "192.168.0.2",
    "StartTime": "2023-03-01T01:03:45.034503Z",
    "Samples": [
        {"MinRTT": 1.23, "RTT": 3.45, "RTTVar": 2.34},
        {"MinRTT": 3.21, "RTT": 5.43, "RTTVar": 4.32}
    ]
}
```

While JSON allows mixed type arrays (e.g. `["a", 1, {}]`), BigQuery does not.
Any array types must use identical repeated element types, defined by the result
SCHEMA.

Once written to disk, the jostler will read the JSON results and bundle them
together into larger JSONL files. Once enough data has been read or enough time
has passed, the JSONL bundle is uploaded to GCS and the local JSON files that
were part of the bundle are removed.

The content of your measurement data will be wrapped by the jostler ["standard
columns"][stdcolumns]. Your measurement result will always be within the `raw`
record.

[stdcolumns]: https://github.com/m-lab/jostler#221-standard-columns

### JSON Schema for Measurement Data

A JSON schema is a JSON array that contains nested column definitions. Every
column definition must include:

* The column's name, e.g. "latitude".
* The column's type, e.g. `FLOAT`, `RECORD`, etc. See all [type options][tablefieldschema].
* The column mode for array types, i.e. `REPEATED`. Do not use `REQUIRED`.

The column definition may also contain:

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
      "description": "Unique measurement identifier, may be joined with other tables",
      "name": "UUID",
      "type": "STRING"
    },
    {
      "description": "Server name or IP for latency measurement",
      "name": "Server",
      "type": "STRING"
    },
    {
      "description": "Client name or IP for latency measurement",
      "name": "Client",
      "type": "STRING"
    },
    {
      "description": "Measurement start time",
      "name": "StartTime",
      "type": "TIMESTAMP"
    },
    {
      "description": "Latencies observed during measurement, sourced from TCPINFO",
      "fields": [
        {
          "description": "Current RTT of connection",
          "name": "RTT",
          "type": "FLOAT"
        },
        {
          "description": "Current RTT variance of connection",
          "name": "RTTVar",
          "type": "FLOAT"
        },
        {
          "description": "Minimum RTT over the life of connection",
          "name": "MinRTT",
          "type": "FLOAT"
        }

      ],
      "mode": "REPEATED",
      "name": "Samples",
      "type": "RECORD"
    }
]
```

### Creating Schema Files

#### Using Golang bigquery.InferSchema

If your service is written with Golang, you can use [`bigquery.InferSchema`][1]
with a Go structure to produce usable schema file. For example, the NDT server
includes this logic to produce a JSON schema from the ndt7 structure.

```go
type Sample struct {
    MinRTT float64
    RTT    float64
    RTTVar float64
}

type Measurement struct {
    UUID      string
    Server    string
    Client    string
    StartTime time.Time
    Samples   []Sample
}

// Generate and save schema for autoloading.
row := Measurement{}
sch, err := bigquery.InferSchema(row)
rtx.Must(err, "failed to generate measurement schema")
b, err := sch.ToJSONFields()
rtx.Must(err, "failed to marshal schema")
ioutil.WriteFile("schema.json", b, 0o644)
```

[1]: https://pkg.go.dev/cloud.google.com/go/bigquery#InferSchema

#### Generating Schema from Sample JSON Object

The `bq` command included in the `google-cloud-sdk` supports creating tables
with schemas inferred from a provided JSON object. To use this method, you need
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
bq load --autodetect --source_format NEWLINE_DELIMITED_JSON \
    mlab-sandbox:foo.bar1 ./bar1-sample.json
bq show --format prettyjson mlab-sandbox:foo.bar1 | jq .schema.fields
```

Manually inspect the resulting schema for errors.

[jq]: https://stedolan.github.io/jq/

#### Manually Creating Schema

Since the schema file is JSON, it can be created manually. However, care must be
taken to ensure that no new fields are added to the measurement output without
also being added to the schema file.
