#!/bin/bash

# This is a helper script for e2e testing by performing the following:
#
#   1. Building jostler.
#   2. Creating the required directories and files.
#   3. Invoking jostler.
#
# See README.txt in this directory for additional details.

set -eu

readonly E2E_DIR="e2e"
readonly SPOOL_DIR="$E2E_DIR/local/var/spool"
readonly GCS_DIR="$E2E_DIR/gcs/autoload/v1"

main() {
	if [[ -z "$EXPERIMENT" || -z "$DATATYPE" ]]; then
		echo both EXPERIMENT and DATATYPE must be set
		exit 1
	fi
	mkdir -p "$PWD/$SPOOL_DIR/datatypes" "$PWD/$SPOOL_DIR/$EXPERIMENT/$DATATYPE"
	cp cmd/jostler/testdata/datatypes/foo1-valid.json "$PWD/$SPOOL_DIR/datatypes/$DATATYPE.json"
	tree "$E2E_DIR"

	if [[ $# -gt 0 && "$1" == "-c" ]]; then
		readonly LOCAL_DATA_DIR="/$SPOOL_DIR"
		readonly GCS_DATA_DIR="/$GCS_DIR"
		cmd=container_run
	else
		readonly LOCAL_DATA_DIR="$PWD/$SPOOL_DIR"
		readonly GCS_DATA_DIR="$PWD/GCS_DIR"
		cmd=native_run
	fi
	readonly JOSTLER_FLAGS=(
			"-gcs-local-disk"
			"-mlab-node-name"       "$EXPERIMENT-mlab1-lga01.mlab-sandbox.measurement-lab.org"
			"-gcs-bucket"           "newclient,download,upload"
			"-gcs-data-dir"         "$GCS_DATA_DIR"
			"-local-data-dir"       "$LOCAL_DATA_DIR"
			"-experiment"           "$EXPERIMENT"
			"-datatype"             "$DATATYPE"
			"-datatype-schema-file" "$DATATYPE:$LOCAL_DATA_DIR/datatypes/$DATATYPE.json"
			"-bundle-size-max"      "1024"
			"-bundle-age-max"       "10s"
			"-missed-age"           "20s"
			"-missed-interval"      "15s"
			"-verbose"
		)
	"$cmd"
}

container_run() {
	execute docker build -t jostler:latest .
	execute docker run -it -v "$PWD/$E2E_DIR:/$E2E_DIR" jostler:latest "${JOSTLER_FLAGS[@]}"

}

native_run() {
	execute go build -o . ./cmd/jostler
	execute ./jostler "${JOSTLER_FLAGS[@]}"
}

execute() {
	echo "$@"
	"$@"
}

main "$@"
