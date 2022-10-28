#!/bin/bash
#
# If you are not a `jostler` developer, you can ignore this script.
#
# This script is solely for testing purposes and is needed because
# BigQuery table creation and deletion cannot (and should not) be done
# with `jostler`.
#
# For each BigQuery table, the script fetches its full schema to extract
# the schema of its datatype.  The extraction is easy if the table does
# not have M-Lab's standard columns.  If the table has standard columns,
# they must be manually deleted.
#
# Once there is a datatype schema, the script runs `jostler` in the
# interactive mode (-local) to create and save a new table schema with
# its own standard columns (see ../api).
#
# Finally, the script runs `bq` to create a BigQuery table in order to
# verify the table schema `jostler` saved is valid.
#

set -eu

readonly BQ_TABLES=(
	"mlab-sandbox:${USER}.hello1"
	"mlab-sandbox:${USER}.foo1"
	"mlab-sandbox:ndt.scamper1" # has standard columns
	"mlab-cloudflare:speedtest.speed2" # doesn't have standard columns
)

readonly COMMON_FLAGS=(
	-verbose
	-local
	-gcs-bucket pusher-mlab-sandbox
	-experiment ndt
)

main() {
	build
	run_tests
}

build() {
	execute go build -race -o jostler ../cmd/jostler
}

run_tests() {
	local table
	local datatype

	for table in "${BQ_TABLES[@]}"; do
		datatype="${table##*.}"

		# Fetch the table's full schema.
		execute bq show --format prettyjson "${table}" > "${table}.full"

		# Extract the "schema" field from the table's full schema.
		execute jq .schema.fields "${table}.full" | execute tail -n +1 > "${table}.datatype"

		# Tell the user to verify the extracted datatype schema
		# does not include standard columns.
		echo edit "${table}.datatype" to remove it does not include standard columns
		read -r -p "hit Enter when ready..."

		# Have jostler create a table schema with standard columns
		# and the extracted datatype schema and save it as a JSON file.
		execute ./jostler "${COMMON_FLAGS[@]}" -datatype "${datatype}" -schema-file "${datatype}:${table}.datatype"

		# Now verify BigQuery can create a table with the schema
		# file that jostler created and saved.

		# Delete the table for this datatype if it already exists.
		if bq show --project_id mlab-sandbox "${USER}.${datatype}" >/dev/null 2>/dev/null; then
			echo deleting the existing "${USER}.${datatype}" table
			execute bq rm --project_id mlab-sandbox --table "${USER}.${datatype}"
		fi

		# Create a table with the schema that jostler saved.
		execute bq mk --project_id mlab-sandbox --table "${USER}.${datatype}" "${datatype}-schema.json"

		# Show the newly created table's full schema.
		execute bq show --project_id mlab-sandbox --format prettyjson "${USER}.${datatype}"

		# Delete the table (bq rm prompts the user to verify).
		execute bq rm --project_id mlab-sandbox --table "${USER}.${datatype}"
	done
}

execute() {
	(>&2 echo "$@")
	"$@"
	(>&2 echo)
}

main "$@"
