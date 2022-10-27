#!/bin/bash

set -eu

readonly TABLES=(
	mlab-sandbox:saied.hello1
	mlab-sandbox:saied.foo1
	mlab-sandbox:ndt.scamper1
	mlab-cloudflare:speedtest.speed2
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
	#shellcheck disable=SC2046
	execute go build -race -o jostler $(find ../cmd/jostler -name '*.go' | grep -v '_test\.go')
}

run_tests() {
	local table
	local datatype

	for table in "${TABLES[@]}"; do
		datatype="${table##*.}"

		# fetch the table's full schema
		echo "bq show --format prettyjson ${table} > ${table}.full"
		bq show --format prettyjson "${table}" > "${table}.full"
		echo

		# extract the "schema" field from the table's full schema
		echo "jq .schema.fields < ${table}.full | tail -n +1 > ${table}.datatype"
		jq .schema.fields < "${table}.full" | tail -n +1 > "${table}.datatype"
		echo

		echo edit "${table}.datatype" to make sure it does not include standard columns
		read -r -p "hit Enter when ready..."

		# have jostler save a table schema with standard columns and the datatype schema in the raw field
		execute ./jostler "${COMMON_FLAGS[@]}" -datatype "${datatype}" -schema-file "${datatype}:${table}.datatype"

		# delete the tabel if it already exists
		if bq show --project_id mlab-sandbox "${USER}.${datatype}" >/dev/null 2>/dev/null; then
			echo deleting the existing "${USER}.${datatype}" table
			execute bq rm --project_id mlab-sandbox --table "${USER}.${datatype}"
		fi

		# create a table with the schema that jostler saved
		execute bq mk --project_id mlab-sandbox --table "${USER}.${datatype}" "${datatype}-schema.json"

		# show the new table's full schema
		execute bq show --project_id mlab-sandbox --format prettyjson "${USER}.${datatype}"

		# delete the table
		execute bq rm --project_id mlab-sandbox --table "${USER}.${datatype}"
	done
}

execute() {
	echo "$@"
	"$@"
	echo
}

main "$@"
