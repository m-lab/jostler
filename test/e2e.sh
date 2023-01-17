#!/bin/bash

set -eux

if [[ -z "$EXPERIMENT" || -z "$DATATYPE" ]]; then
	echo both EXPERIMENT and DATATYPE must be set
	exit 1
fi

go build -o . ./cmd/jostler

readonly E2E_SPOOL_DIR=$(pwd)/e2e/local/var/spool
readonly E2E_DATATYPE_SCHEMA_DIR=$E2E_SPOOL_DIR/datatypes
readonly LOCAL_DATA_DIR=$E2E_SPOOL_DIR/$EXPERIMENT/$DATATYPE

mkdir -p $E2E_DATATYPE_SCHEMA_DIR $LOCAL_DATA_DIR
cp cmd/jostler/testdata/datatypes/foo1-valid.json $E2E_DATATYPE_SCHEMA_DIR/$DATATYPE.json

git clean -ndx

./jostler \
	-local-disk \
	-mlab-node-name $EXPERIMENT-mlab1-lga01.mlab-sandbox.measurement-lab.org \
	-gcs-bucket newclient,download,upload \
	-gcs-home-dir e2e/gcs/autoload/v1 \
	-local-data-dir $E2E_SPOOL_DIR \
	-experiment $EXPERIMENT \
	-datatype $DATATYPE \
	-datatype-schema-file $DATATYPE:$E2E_DATATYPE_SCHEMA_DIR/$DATATYPE.json \
	-bundle-size-max 1024 \
	-bundle-age-max 10s \
	-missed-age 20s \
	-missed-interval 15s \
	-verbose
