There are two files in this test directory to help with integration
testing.

test_schema.sh is a bash script that test table schema creation from
datatype schemas.

data.go is a very simple Go program that mimics a measurement service
by creating measurement data files that jostler will bundle and upload.
data.go can be started in one terminal and jostler can be started in
another terminal as follows:

[Term A]
$ go run data.go

[Term B]
./jostler -verbose \
	-mlab-node-name ndt-mlab1-lga01.mlab-sandbox.measurement-lab.org \
	-gcs-bucket fake,newclient,download,upload \
	-data-home-dir ./cmd/jostler/testdata/spool \
	-experiment jostler \
	-datatype foo1 \
	-datatype-schema-file foo1:cmd/jostler/testdata/datatypes/foo1-valid.json

Because the -gcs-bucket flag is set to fake,newclient,download,upload,
jostler will use testhelper's fake GCS implementation which uses the
local filesystem instead of Google Could Storage.  This makes testing
and debugging a lot easier.
