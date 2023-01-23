# Autoloading Measurement Data into BigQuery

## 1. Objective

Deploy measurement services with fast publication of measurement data
to BigQuery (**autoloading**) by obviating the need to parse and
process measurement data. Operator intervention should be minimal.

## 2. Scope

The scope of this design is limited to _new measurement services_
(**new service** to be deployed in the M-Lab platform that
will generate _new measurement data format_ (**new format**)
that conform to the requirements of [Loading JSON data from Cloud
Storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json).

For now, all measurement data produced by the M-Lab's _existing_
measurement services will continue to be parsed and processed as they
currently are.  Once autoloading is fully operational, the existing
measurement services may also benefit from this simplification.

## 3. Terminology

**Field**: A JSON field in data mapping to a column in a BigQuery table.
**Column**: A BigQuery table column storing a JSON field value.
**Experiment**: Synonym for measurement service (e.g., NDT)
**Datatype**: The name and version of measurement data that specifies
its structure (**datatype schema**).  For example, `ndt5` is NDT
protocol 5 and `ndt7` is NDT protocol 7.
**Bundle**: A collection of measurement data files in a single file in
JSON Lines (**JSONL**) format

**Node name**: Combination of machine name and site name (e.g., `mlab3-par05`)

**GCS object**: A resource within GCS (similar to a file in a
filesystem). GCS has a flat namespace so the object `folder1/file.txt`
is _not_ inside folder `folder1`; rather, `folder1` is part of
the object's name creating the illusion of a folder hierarchy
([details](https://cloud.google.com/storage/docs/folders)).

## 4. Motivation

Autoloading measurement data into BigQuery will greatly improve the
process of deploying third-party new measurements in the M-Lab platform
without requiring support from the M-Lab team for changes in the pipeline
to parse and process the new format.  Specifically, new autoloading
approach will have the following advantages over the existing approach:

1. No need to develop parsers (development time down to zero).
2. No need to have an operator configure
<code>[gardener](https://github.com/m-lab/etl-gardener)</code>
(configuration time down to zero).
3. Possibly no need to have an operator configure e2e monitoring per datatype.

## 5. Background

### 5.1. M-Lab architecture

From a measurement perspective, the current M-Lab architecture consists
of the following components:

* Measurement platform consisting of physical and virtual machines
(**nodes**)
* Cloud Storage (GCS) buckets 
* ETL data pipeline (**pipeline**)
* BigQuery tables and views

The measurement platform collects measurement data (**data**) from
running measurement services and uploads archives of data in compressed
tar format to GCS as shown in the following diagram.  The agent that
creates archives of data in the platform nodes and uploads them to GCS
is M-Lab's <code>[pusher](https://github.com/m-lab/pusher)</code>.

The pipeline, in turn, consists of several components and stages to
parse the uploaded archives and load archive's data into BigQuery.
For example, the following diagram shows how the pipeline processes data
produced by [NDT](https://www.measurementlab.net/tests/ndt/) measurements.

1. Pipeline's <code>[parser](https://github.com/m-lab/etl/tree/main/parser)</code>
reads from the public archive and writes JSONL results to temporary GCS.
2. Pipeline loads JSONL from GCS into temporary tables in BigQuery.
3. Pipeline's <code>gardener</code> performs deduplication from
temporary to raw tables.
4. Pipeline's <code>gardener</code> materializes join of raw
<code>ndt7</code> and raw <code>annotation</code>.

Because BigQuery supports loading data in JSONL format from GCS, it
is possible to bypass the parser for the new format data that conforms
to BigQuery requirements.  In other words, once a new measurement has
produced its data, it will be autoloaded into BigQuery without manual
intervention or parsing delays as illustrated in the following diagram:

### 5.2. Schemas

The word schema in M-Lab documentation and source code can be confusing
because sometimes it refers to a _datatype_ schema and other times it
refers to a _table_ schema where the table name is the datatype name.

A datatype schema is the schema of the measurement data whereas a table
schema is the schema of a BigQuery table that by convention includes
M-Lab's standard columns, with the datatype schema in its `raw` field.
For example, the `scamper1` _table_ includes M-Lab's standard columns
`id`, `parser`, `date`, and `raw`, and the `raw` column contains the
`scamper1` _datatype_.

A BigQuery _table schema_, then, is the sum of _M-Lab standard columns
schema_ and the _datatype schema_.  That is:

```
	table schema = M-Lab standard columns schema + datatype schema
```

Note that measurement data that is _not_ generated on M-Lab nodes (e.g.,
Cloudflare's speed data) will not include all M-Lab standard columns.

BigQuery tables are identified as `<project>:<dataset>.<table>`.
As mentioned earlier, by convention, table names have the same as the
datatype they store.  For example, the `mlab-oti:ndt.ndt7` table stores
`ndt7` datatype, and you can use the following command to see its schema:

```
    $ bq show --format prettyjson mlab-oti:ndt.ndt7
```

A breaking backward-incompatible change in the schema requires defining
a new datatype.  For example, a breaking change in the `foo1` datatype
will require a _new_ datatype called `foo2`.

BigQuery supports the following methods for specifying table schemas.

1. **[Inline](https://cloud.google.com/bigquery/docs/schemas#bq)**:
Table schemas can be manually supplied inline in the format
`field:data_type,field:data_type` when creating a table or loading data.
However, when specifying the table schema on the command line, it is
not possible to include a `RECORD (STRUCT)` type, column description,
or column mode (all modes default to `NULLABLE`).
2.  **[Auto-detection](https://cloud.google.com/bigquery/docs/schema-detect)**:
Let BigQuery infer the table schema when loading data in JSON format.
In this mode, BigQuery selects a random file in the data source and
scans up to the first 500 rows of data to use as a representative
sample. BigQuery then examines each field and attempts to assign a data
type to that field based on the values in the sample.
3.  **[File](https://cloud.google.com/bigquery/docs/schemas#creating_a_json_schema_file)**:
A JSON table schema file consists of a JSON array that must contain a
`name` and a `data type` for each column.  The JSON array can also
optionally contain other column attributes such as `mode`, `fields` if
it is a `STRUCT` type, `description`, `policy` tags (used for field-level
access control), etc.

The inline method is suitable only for very simple tables and, therefore,
is not an option for autoloading.  The auto-detection method to implicitly
infer table schema is not a reliable option for autoloading because
there is no guarantee the first 500 rows represent all columns.

The file method is reliable and robust because it allows us to explicitly
specify a table schema in JSON format.  Being explicit is important
because not every measurement data will necessarily include all fields
(columns) of the datatype schema and many have non-trivial datatype
schemas.  Also, this approach makes table schema compatibility verifiable.

## 6. Requirements

Autoloading requirements are divided into two main categories:
requirements that must be met by third-parties that wish to deploy new
measurements and requirements that must be met by M-Lab architecture.

### 6.1. Third-Party requirements

The words should and must in the following lines should be
interpreted as defined [here](https://www.ietf.org/rfc/rfc2119.txt).
M-Lab's website has an [Experiment
Requirements](https://www.measurementlab.net/experimenter-requirements-guidelines/#experiment-requirements)
section that includes many of the following requirements.

The new measurements:

1. MUST provide their datatype schema in well-formed JSON format.
2. MUST generate well-formed data in JSON format that would meet these BigQuery [limitations](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#limitations).  These limitations include the note: "_When you load CSV or JSON data, values in DATE columns must use the dash (-) separator and the date must be in the following format: YYYY-MM-DD (year-month-day)._"
3. SHOULD create new format files in the following predefined directory:
        ```
        /var/spool/<experiment>/<datatype>/<yyyy>/<mm>/<dd>
        ```
4. MUST not use the above predefined directory as working or temporary directory for creation of arbitrary data files.
5. MUST generate filenames with the alphabet, numbers, underscore (_), colon (:), period (.), and dash (-); the filename should start with an alphanumeric character and cannot have more than one period in a row.
6. MUST not re-open new format files for writing.
7. MUST not keep a file open without writing to it for more than a few
minutes (see the section Missed data files for the reason).
8. MUST not reuse new format filenames (i.e., each new format filename must be unique).
9. MUST not assume that new format and any other files it creates will persist on the local disk for any duration of time because local files will be deleted after a bundle is uploaded to GCS.

### 6.2. M-Lab requirements

1. Autoloading MUST not require any configuration updates in the pipeline.
2. Autoloading MUST not break the existing functionality of `pusher`, `parser`, and `gardener`.
3. Autoloading should not require major changes to how `pusher` is invoked for the existing measurement data.
4. Autoloading should not parse and interpret new format filenames.
5. Autoloading MUST parse new format contents to verify it is well-formed JSON.
6. Autoloading MUST copy individual new format contents into a compressed JSONL bundle and upload the bundle to predefined GCS buckets `{pusher|jostler}-mlab-{sandbox,staging,oti}.`
7. Autoloading MUST follow the [existing conventions for GCS object names](https://cloud.google.com/storage/docs/naming-objects).
8. Autoloading SHOULD conform to the conventions of M-Lab's [Standard Top-Level BigQuery Columns](https://docs.google.com/document/d/1WkQiukzgWjlIslcarXvyzAcx_rhwmnjDSi8brXS6iAw/edit#heading=h.qid3osrp8ord) (**standard columns**) and, in particular, the section Raw Top-Level Columns except for the parser column.
9. Pipeline SHOULD automatically detect new files that are uploaded to predefined GCS buckets.
10. Loading of new datatypes MUST happen automatically.
    1. This includes noticing and applying compatible schema updates
11. Updates to the archive buckets in GCS MUST be reflected in the corresponding BigQuery tables within the following time periods:
    2. Three hours for files uploaded within the last 2 days.
    3. One week for files uploaded within the last 2 months.
    4. One month for the entire archive.
12. File additions, updates, and removals MUST be supported automatically.
13. Monitoring and alerting of new datatypes MUST happen automatically.
14. Should allow future functionality for the Gardener to join autoloaded types.

## 7. Design

There are two areas in the M-Lab architecture that need changes to support autoloading:

1. **Nodes**: an agent to generate compressed JSONL bundles from individual new format files and upload them to GCS.
2. **Pipeline**: an agent to discover and load compressed JSONL bundles from GCS into BigQuery without further processing.

The reason for having an uploader agent on M-Lab nodes is that measurement
services write their results to the local filesystem (i.e., they do not
directly upload to GCS).  Therefore, there must be an agent on M-Lab
nodes to authenticate with GCS and upload measurement data.  Because GCS
uploads incur an overhead which can be prohibitive with many small files,
the agent puts individual files in a bigger file (a `tar` archive or a
JSONL bundle) that is compressed before uploading.

### 7.1. Uploader agent on the nodes

As mentioned earlier, the existing uploader agent for the existing
measurement services that creates archives of data and uploads them
to GCS is M-Lab's `pusher`.  To support autoloading, we can extend
the functionality of `pusher` to recognize the new format and treat
it differently from the existing data but changing `pusher` is risky
as it processes _millions_ of files on a daily basis.  A disruption
in `pusher`'s operation can potentially lead to major data loss or
time-consuming manual processes to restore operation.  It is therefore
preferable to write a new uploader agent in order to avoid disruptions to
the current measurements.  Once the new agent is fully functional and has
reached production quality, we can decide if it would be beneficial to
integrate the tools together.  We call the new uploader agent `jostler`
to differentiate it from `pusher`.

#### 7.1.1. Differences between `pusher` and `jostler`

There are several differences between the two uploader agents as follows:

1. `pusher` creates a compressed tar archive of _any_ file it finds
(text, binary, JSON, etc.) whereas `jostler` bundles only JSON files
and creates a compressed JSONL bundle (not a `tar` file).
2. In addition to new format files that were included in the bundle,
`jostler` removes other files in the predefined directory of the local
filesystem that were _not_ included in the bundle (files that were not
`.json`, could not be read, or were invalid JSON).  This is consistent
with the requirement that new measurements should generate _only_
well-formed JSON files.
3. `jostler` knows about datatype schema and _requires_ it to build a
BigQuery table schema which is uploaded as a predefined object to GCS.
4. `jostler` creates and uploads an index of the files in a bundle.
The index will make it very convenient to find out which bundle contains
a particular new format file.

### 7.2.`jostler` operation

`jostler` will support two modes of operation:

1. A **short-lived interactive local mode** to create a BigQuery
table schema file for a given datatype.  As mentioned before, the
table schema file is built using the standard columns and the datatype
schema. This mode is mainly meant for an operator to examine and verify
the table schema before creating a table.
2. A **long-lived non-interactive daemon mode** to serve as an
upload agent on M-Lab's nodes.

In the long-lived non-interactive mode, `jostler` will run as a sidecar
container of the new measurement container.  The new measurements
will write new format files in JSON format to a predefined location
on the local filesystem as described earlier and `jostler` will use
<code>[inotify](https://man7.org/linux/man-pages/man7/inotify.7.html)</code>
to monitor filesystem events in the predefined location.
<code>jostler</code> will read individual new format files, bundle them
together, compress the bundle, and upload the bundle to GCS.  After a
successful upload, <code>jostler</code> will delete from the local
filesystem new format and other files that the new measurement generated.

As mentioned earlier, any files that were not included in a bundle due to
any error such as wrong extension (not `.json`), read error, or invalid
JSON will also be deleted from the local filesystem in order to avoid
filling up the node's disk space.

There are two configurable parameters that control triggering of an
upload operation:

1. Bundle size (e.g., 30 megabytes)
2. Bundle age (e.g., 3 hours)

Once a bundle reaches its maximum allowable size or age, it will be
uploaded to GCS.

### 7.3. Bundle names

As mentioned in the requirements, the location of new format files is
predefined in the new measurement container as follows:

```
    /var/spool/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<new-format-data>
```

The reason new format pathnames must follow the above convention is that
upload agents (`pusher` and `jostler`) use the same string of the pathname
after `/var/spool` as a prefix for GCS object names.  For details,
see [Uniform Names: Experiments by Any Other Name [Would Not Be As
Sweet]](https://docs.google.com/document/d/1BPt11RK5x6FZTXvWbZaC_GhSlH9vHzhrRP6ca0TBosI/edit#).

For example, `pusher` creates object names prefixed by
`ndt/scamper1/2022/09/12`

in the `pusher-mlab-oti` bucket for traceroute data (`scamper1` datatype)
generated as a sidecar service of NDT measurements on 2022/09/12 as we
can see below:

```
    $ gsutil ls gs://pusher-mlab-oti/ndt/scamper1/2022/09/12
    gs://pusher-mlab-oti/ndt/scamper1/2022/09/12/20220912T143138.409697Z-scamper1-mlab2-gru01-ndt.tgz
    gs://pusher-mlab-oti/ndt/scamper1/2022/09/12/20220912T143139.800575Z-scamper1-mlab3-gig03-ndt.tgz
    ...
```

`jostler` will upload JSONL bundles to a GCS bucket specified by
a flag which can be the same as the current `pusher`'s buckets
`pusher-mlab-{sandbox,staging,oti}`.  And because `jostler`'s
GCS object names have the `autoload/<version>` prefix before
`<experiment>/<datatype>/...` they will be easily distinguished from
`pusher`'s objects:

```
    autoload/<version>/<experiment>/<datatype>/<yyyy>/<mm>/<dd>
```

The purpose of `autoload/<version>` in the prefix of the object name
is to support breaking changes to autoloading implementation.

Each bundle will have the following naming convention:

```
    prefix=autoload/<version>/<experiment>/<datatype>/<yyyy>/<mm>/<dd>
    <prefix>/<timestamp>-<datatype>-<node-name>-<experiment>.jsonl.gz
```

### 7.4. Bundle contents

#### 7.4.1. Standard columns

Each bundle will consist of individual JSON objects (new format files),
one per line, and each line will include a subset of standard columns
in the first version (v1) of autoloading.  With respect to the standard
columns, it's important to highlight the following:


* Since the main objective of autoloading is to avoid parsing, there
will be no `parser` record.  Instead, there will be an `archiver` record
that `jostler` will add by wrapping raw JSON from new format files
within an outer record.  In this way `jostler` would make it easier
for the new measurement to satisfy the standard columns requirement.
But third-parties that don't use `jostler` would still be better if they
included fields like `date` (and others in time when we specify more).
* It will be helpful to have an `id` field.  In fact, this will be a
requirement in the future if we ever want to join autoloaded data with,
say, the annotation data. However, since this requires more semantic
awareness of the raw JSON and some way of specifying the format of
the `id`, it is not a requirement for autoloading v1.  Aside from
autoloaded data, we should keep this in mind with the possible future
goal of migrating existing JSON `parser` datatypes to be autoloaded.
The `id` field could be the filename minus any filename extension to
encourage services to name files with the UUID or similarly meaningful
unique identifier. This would preserve semantic opaqueness of the raw
data while providing a convention to populate the id field.

Version 1 of a JSONL bundle will look like the following, pretty printed,
abbreviated, and showing standard column names in boldface:

```
    {
      "date": "2022/09/29",
      "archiver": {
        "Version": "jostler@0.1.7",
        "GitCommit": "3ac4528",
        "ArchiveURL": "gs://<bucket>/<prefix>/<bundlename>.jsonl.gz",
        "Filename": "<yyyy>/<mm>/<dd>/<filename>.json"
      },
      "raw": {
        "UUID": "1234",
        "MeasurementVersion": "0.1.2",
        "Field1": 42
      }
    }
    {
      "date": "2022/09/29", "archiver": {...},
      "raw": {
        "UUID": "3456",
        "MeasurementVersion": "0.1.2",
        "Field2": 3.14
      }
    }
    {
      "date": "2022/09/29", "archiver": {...},
      "raw": {
        "UUID": "4567",
        "MeasurementVersion": "0.1.2",
        "Field1": 420,
        "Field2": 31.41
      }
    }

```

* <strong><code>date</code></strong> is the date component of the directory
pathname where new format files were discovered.  For example,
the <code>date</code> field of the bundle that contains new format
files in <code>/var/spool/ndt/foo1/2022/09/29</code> will be
<code>2022/09/29</code>.

* <strong><code>archiver</code></strong> defines the details of the
running instance of <code>jostler.</code>

* <strong><code>raw</code></strong> contains individual new format
contents in JSON format without any modification.  The fields
<code>UUID</code>, <code>MeasurementVersion</code>, <code>Field1</code>,
and <code>Field2</code> are simply examples.  The new measurement provider
will decide what fields will be included in their new format.

Notice that not all data fields are necessarily included in each
<code>raw</code> JSON object (new format files).  The above example
shows that <code>Field2</code> and <code>Field1</code> are missing from
the first and the second new format files respectively.

### 7.5. Datatype schema

As mentioned in the [Requirements](#heading=h.x9xgmk803y95), new
measurements should provide the schema of their measurement data as a
file in JSON format.

When `jostler` starts, it looks for datatype schema files of each
specified datatype, generates the corresponding BigQuery table schema
(which includes M-Lab's standard columns), and uploads the table schema
files to GCS.  The location of a datatype schema file can be specified via
a command line flag (`-datatype-schema-file`) but its default location is:

```
    /var/spool/datatypes/<datatype>.json
```

In the interactive mode, the operator can use the `-schema` flag to create
the schema and examine it.  For example, below is the command to create
BigQuery table schemas for tables `foo1` and `bar1`.  In this example,
`jostler` is told to look for `foo1`'s measurement data schema in the
default location and for `bar1`'s in `/path/to/bar1.json`.

```
    $ ./jostler -schema -datatype foo1 -datatype bar1 \
        -datatype-file bar1:/path/to/bar1.json
```

`jostler` uploads table schema files to GCS as the following objects:

```
    autoload/v1/tables/<experiment>/foo1-table.json
    autoload/v1/tables/<experiment>/bar1-table.json
```

As mentioned earlier, the purpose of version `v1` is to support breaking
changes to autoloading implementation (i.e., conventions agreed on between
`jostler` and the loader agent in the pipeline).

### 7.6. Index bundles

For every JSONL bundle that `jostler` uploads to GCS, it will also upload
an index file also in JSONL format that contains the list of filenames
contained in the bundle in the same order that new format data appears
in the `raw` fields of the bundle.

`jostler` creates index files as a special datatype of `index1` so the
autoload agent in the pipeline does not have to distinguish between
measurement data files and index files.  In other words, as far as the
pipeline is concerned, `index1` is just another datatype.

Index bundles will have the same name as the bundle they describe.

### 7.7. Default paths and object names

In summary, by default:

1. Measurement data files will be read from the local filesystem at:
    ```
    /var/spool/<experiment>/<datatype>/<yyyy>/<mm>/<dd>
    ```
2. Datatype schema files will be read from the local filesystem at:
    ```
    /var/spool/datatypes/<datatype>.json
    ```
3. Table schema files will be uploaded to GCS as:
    ```
    autoload/v1/tables/<experiment>/<datatype>.table.json
    ```
4. JSONL files will be uploaded to GCS as:
    ```
    autoload/v1/<experiment>/<datatype>/date=<yyyy>-<mm>-<dd>/<timestamp>-<datatype>-<node-name>-<experiment>.jsonl.gz

    ```
It should be clear that, since index files have a datatype of `index1`,
their schema and JSONL files will be uploaded to GCS as:

```
    autoload/v1/tables/<experiment>/index1.table.json
    autoload/v1/<experiment>/index1/date=<yyyy>-<mm>-<dd>/<timestamp>-<datatype>-<node-name>-<experiment>.jsonl.gz
```

### 7.8. GCS authentication

To be written by.

### 7.9. `jostler` configuration

**GCS configuration**

* bucket name: for example` pusher-mlab-{sandbox,staging,oti}`
* home folder: object name starts with this string (e.g., `autoload/v1)`
* M-Lab node name:` `parsed and used in object names (examples in 

**Bundle configuration**
* maximum size: maximum size before it is uploaded
* maximum age: maximum duration since a bundle was created in memory until it is uploaded

**Filesystem configuration**
* home directory: directory under which measurement data is created (e.g., `/var/spool`)
* extensions: filename extensions of interest (default `.json`); other files will be ignored
* experiment: name of the measurement service (e.g., `ndt`)
* datatypes: name(s) of the datatype(s) the experiment generates (e.g., `scamper1`)
* minimum file age: minimum duration since a file's last modification time before it is considered a missed data file
* scan interval: the interval for scanning filesystem for missed files

**Execution**
* flush timeout: maximum duration for flushing active bundles to GCS before exiting
* schema: run in the interactive mode and create schema files
* verbose: enable verbose mode for more logging

### 7.10. `jostler` architecture

`jostler` architecture will consist of two major packages.  One package,
called `watchdir`, will watch a directory where new format JSON files
are created.  The other package, called `bundlejson`, will bundle these
files into compressed JSONL files and upload them to GCS.

`watchdir` will ignore files that do not have a `.json` suffix and
`bundlejson` will ignore files that are not in proper JSON format.
As mentioned earlier, `jostler` is different from `pusher` by not
indiscriminately including all files in the bundle regardless of
their content.  This behavior of `jostler` will provide [better
security](https://github.com/m-lab/pusher/blob/main/DESIGN.md#7-security-considerations).

It is highly desirable that `jostler` guarantees it will not upload the
same new format file more than once.  With this guarantee there will be
no need to deduplicate data.  Due to asynchronous pod reboots and GCS
failures, the feasibility of this guarantee is currently unclear but
every effort will be made to obviate the need for data deduplication.

### 7.11. Concurrency and shared data

To be written.

### 7.12. Missed data files

For all _planned_ reboots, upload agents on M-Lab nodes will have a
duration to flush out their active data and wrap up gracefully so that
no files are missed.  For `pusher`, the duration is specified with the
`-sigtermWait` flag and for `jostler` it will be specified with the
`-flushTimeout` flag.

However, because pods can have _unplanned_ restarts at any
time, it is possible for `jostler` (or any other agent) to
miss the Writable file was closed (`IN_CLOSE_WRITE`) or
File was moved to (`IN_MOVED_FROM`) `inotify` events.
Also if too many events occur at once, the `inotify` event
queue can overflow and lose some events (see [Limitations and
caveats](https://man7.org/linux/man-pages/man7/inotify.7.html)).
Additionally, if upload to GCS fails, the individual new format files
that were in the bundle will not be deleted.

When a file's last modification time is more than a configurable
duration (e.g., 2 hours), `jostler` assumes it either missed the file's
`IN_CLOSE_WRITE` or `IN_MOVED_FROM` event or uploading to GCS wasn't
successful.  In cases like this, `jostler` considers the file eligible
for upload.  This also means that files that are open but are not modified
for more than the configurable duration will be uploaded _prematurely_.
This is why it is required that new measurements should not keep a file
open without writing to it for more than a few minutes.

## Related Material

* [Pusher Design Document](https://github.com/m-lab/pusher/blob/main/DESIGN.md)
* [BigQuery - Overview of Cloud Storage](https://cloud.google.com/bigquery-transfer/docs/cloud-storage-transfer-overview)
* [BigQuery - Loading JSON data from Cloud Storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json)
* [BigQuery - Loading nested and repeated JSON](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#loading_nested_and_repeated_json_data)
* [BigQuery - Batch loading data](https://cloud.google.com/bigquery/docs/batch-loading-data)
* [BigQuery - Quotas and limits](https://cloud.google.com/bigquery/quotas#load_jobs) 

# APPENDIX

## Current measurement data filenames

The existing M-Lab measurement services follow different naming conventions for storing data on the node's local disk.  For example, NDT7 measurements store `ndt7` (download and upload), `annotation`, `hopannotation1`, `pcap`, `scamper1`, and `tcpinfo` datatypes in files named as shown below respectively:


```
    ndt7-download-20220805T000129.186460411Z.ndt-b8ljl_1659653925_0000000000000827.json.gz
    ndt7-upload-20220805T000141.791230722Z.ndt-b8ljl_1659653925_000000000000082B.json.gz
    ndt-b8ljl_1659653925_0000000000000115.json
    20220804T230445Z_ndt-b8ljl_116.119.73.96.json
    ndt-b8ljl_1659653925_00000000000001B2.pcap.gz
    20220804T233322Z_ndt-b8ljl_1659653925_00000000000004BC.jsonl
    ndt-b8ljl_1659653925_0000000000000073.00005.jsonl.zst
```


The above filenames are various combinations of UUID, timestamp, and test name.


## Current GCS object names

Unlike the filenames of measurement data that follow different naming conventions, object names in GCS created by `pusher` follow a consistent naming convention.  For example, `pusher` creates an object in the `pusher-mlab-oti` GCS bucket with the following name:


```
    20220804T144156.694010Z-pcap-mlab2-ams08-ndt.tgz
```
