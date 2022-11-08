There are two files in this test directory to help with integration
testing.

test_schema.sh is a bash script that test table schema creation from
datatype schemas.

data.go is a very simple Go program that mimics a measurement service
by creating measurement data files that jostler will bundle and upload.
data.go can be started in one terminal and jostler can be started in
another terminal as follows:


[Term A]
$ pwd
.../jostler
$ cd test
$ go run data.go -sleep 1s


[Term B]
$ pwd
.../jostler
$ mkdir -p cmd/jostler/testdata/spool/jostler/foo1
$ go build -o . ./cmd/jostler
$ ./jostler \
	-mlab-node-name ndt-mlab1-lga01.mlab-sandbox.measurement-lab.org \
	-gcs-bucket fake,newclient,download,upload \
	-data-home-dir $(pwd)/cmd/jostler/testdata/spool \
	-experiment jostler \
	-datatype foo1 \
	-datatype-schema-file foo1:cmd/jostler/testdata/datatypes/foo1-valid.json \
	-bundle-size-max 1024 \
	-bundle-age-max 10s \
	-missed-age 20s \
	-missed-interval 15s

Because the -gcs-bucket flag is set to fake,newclient,download,upload,
jostler will use testhelper's fake GCS implementation which uses the
local filesystem instead of Google Could Storage.  This makes testing
and debugging a lot easier.


[Term C]
$ pwd
.../jostler
$ while :; do tree testdata cmd/jostler/testdata/spool; sleep 1; done

You will see that data files are created by test/data.go in the
subdirectories of:

	.../jostler/cmd/jostler/testdata/spool/jostler/foo1/2022/11

and are deleted by jostler after they are bundled and "uploaded" to the
subdirectories of:

	.../jostler/testdata/autoload/v0/jostler/foo1/2002/11
