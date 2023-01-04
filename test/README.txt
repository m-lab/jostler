There are two files in this test directory to help with integration
and e2e testing.

1. schema.sh is a bash script that can be used for testing table schema
   creation from datatype schemas.  Read the comments at the beginning of
   schema.sh to understand what it does and how it works.

2. data.go is a very simple Go program that mimics a measurement service
   by creating measurement data files for jostler to bundle and upload.

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
	-local-disk \
	-mlab-node-name ndt-mlab1-lga01.mlab-sandbox.measurement-lab.org \
	-gcs-bucket disk,newclient,download,upload \
	-data-home-dir $(pwd)/cmd/jostler/testdata/spool \
	-experiment jostler \
	-datatype foo1 \
	-datatype-schema-file foo1:cmd/jostler/testdata/datatypes/foo1-valid.json \
	-bundle-size-max 1024 \
	-bundle-age-max 10s \
	-missed-age 20s \
	-missed-interval 15s

Because the -local-disk flag is specified, jostler will use testhelper's local
disk storage implementation which mimics downloads from and uploads to
cloud storage (GCS).  This makes testing and debugging a lot easier.


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
