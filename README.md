# Jostler Design

## 1. Background

`jostler` is the uploader agent of the `Autoloading` project.
Please see the design doc
[Autoloading Measurement Results into BigQuery](https://docs.google.com/document/d/1kJ2oy5MAwYBBCq2mVOoJBjq4zU2xiEy1gRAYNoim920/edit)
for details.

## 2.`jostler` operation

`jostler` will support two modes of operation:

1. A **short-lived interactive local mode** to create a BigQuery
table schema file for a given datatype.  The
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

Any files that were not included in a bundle due to
any error such as wrong extension (not `.json`), read error, or invalid
JSON will also be deleted from the local filesystem in order to avoid
filling up the node's disk space.

There are two configurable parameters that control triggering of an
upload operation:

1. Bundle size (e.g., 30 megabytes)
2. Bundle age (e.g., 3 hours)

Once a bundle reaches its maximum allowable size or age, it will be
uploaded to GCS.

### 2.1. Bundle names

The location of new format files is predefined in the new measurement
container as follows:

```
    /var/spool/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<new-format-data>
```

The reason new format pathnames must follow the above convention is
that upload agents,  [`pusher`](https://github.com/m-lab/pusher) and
`jostler`, use the same string of the pathname after `/var/spool`
as a prefix for GCS object names.  For details, see [Uniform
Names: Experiments by Any Other Name [Would Not Be As
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

Each data bundle will have the following naming convention:

```
    prefix=autoload/<version>/<experiment>/<datatype>/<yyyy>/<mm>/<dd>
    <prefix>/<timestamp>-<datatype>-<node>-<experiment>-data.jsonl.gz
```

### 2.2. Bundle contents

#### 2.2.1. Standard columns

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
      "**date**": "2022/09/29",
      "**archiver**": {
        "Version": "jostler@0.1.7",
        "GitCommit": "3ac4528",
        "ArchiveURL": "gs://<bucket>/<prefix>/<bundlename>.jsonl.gz",
        "Filename": "<yyyy>/<mm>/<dd>/<filename1>.json"
      },
      "**raw**": {
        "UUID": "1234",
        "MeasurementVersion": "0.1.2",
        "Field1": 42
      }
    }
    {
      "**date**": "2022/09/29",
      "**archiver**": {
        "Version": "jostler@0.1.7",
        "GitCommit": "3ac4528",
        "ArchiveURL": "gs://<bucket>/<prefix>/<bundlename>.jsonl.gz",
        "Filename": "<yyyy>/<mm>/<dd>/<filename2>.json"
      },
      "**raw**": {
        "UUID": "1234",
        "MeasurementVersion": "0.1.2",
        "Field1": 420
        "Field2": 31.41
      }
    }
    ...
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

### 2.3. Datatype schema

New measurements should provide the schema of their measurement data as
a file in JSON format.

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

The purpose of version `v1` is to support breaking changes to autoloading
implementation (i.e., conventions agreed on between `jostler` and the
loader agent in the pipeline).

### 2.4. Index bundles

For every JSONL bundle that `jostler` uploads to GCS, it will also upload
an index file also in JSONL format that contains the list of filenames
contained in the bundle in the same order that new format data appears
in the `raw` fields of the bundle.

`jostler` creates index files as a special datatype of `index1` so the
autoload agent in the pipeline does not have to distinguish between
measurement data files and index files.  In other words, as far as the
pipeline is concerned, `index1` is just another datatype.

Index bundles will have the same name as the bundle they describe.

### 2.5. Default paths and object names

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
4. JSONL data bundles will be uploaded to GCS as:
    ```
    autoload/v1/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<timestamp>-<datatype>-<node>-<experiment>-data.jsonl.gz
    ```
5. JSONL index bundles will be uploaded to GCS as:
    ```
    autoload/v1/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<timestamp>-<datatype>-<node>-<experiment>-index1.jsonl.gz
    ```

### 2.6. `jostler` configuration

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

### 2.7. `jostler` architecture

`jostler` architecture consists of a public `api` package that defines
standard columns and `index1` datatype, and the following internal packages:

* `internal/gcs`: handles downloading and uploading files to Google Cloud Storage (GCS).
* `internal/jsonlbundle`:  implements logic to process a single JSONL bundle.
* `internal/schema implements logic to handle datatype and table schemas.
* `internal/testhelper`: implements logic to help in unit and integration (e2e) testing.
* `internal/uploadbundle`: implements logic to bundle multiple local JSON files into JSONL bundles and upload to Google Cloud Storage (GCS)
* `internal/watchdir`: watches a directory and sends notifications to its client when it notices a new file.

Files that do not have a .json suffix or are not in proper JSON format
will be ignored.  As mentioned earlier, jostler is different from pusher
by not indiscriminately including all files in the bundle regardless
of their content.  This behavior of `jostler` will provide [better
security](https://github.com/m-lab/pusher/blob/main/DESIGN.md#7-security-considerations).

It is highly desirable that `jostler` guarantees it will not upload the
same new format file more than once.  With this guarantee there will be
no need to deduplicate data.  Due to asynchronous pod reboots and GCS
failures, the feasibility of this guarantee is currently unclear but
every effort will be made to obviate the need for data deduplication.

### 2.8. Concurrency and shared data

To be written.

### 2.9. Missed data files

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
