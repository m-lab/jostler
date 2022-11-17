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
# For convenience, there are valid datatype schema files in this directory
# that can be readily used (instead of querying BigQuery).  If you prefer
# to use these files, just make a copy without the ".valid" suffix.
#
#   $ cp <file>.datatype.valid <file>.datatype
#   $ ./schema.sh
#
# Once there is a datatype schema, the script runs `jostler` in the
# interactive mode (-local) to create and save a new table schema with
# its own standard columns (see ../api).
#
# Finally, the script runs `bq` to create a BigQuery table in the user's
# dataset in order to verify the table schema `jostler` saved is valid.
#

set -eu

readonly PROJECT_ID="mlab-sandbox"

# The `hello1` and `foo1` tables are commented out because, as their
# names suggest, they were trivial tables to help debugging.  They are in
# the script to let you know that you can create your own tables for test
# purposes and do not have to rely on existing tables.
readonly BQ_TABLES=(
	#"${PROJECT_ID}:${USER}.hello1"
	#"${PROJECT_ID}:${USER}.foo1"
	"${PROJECT_ID}:ndt_raw.scamper1" # has standard columns
	"mlab-cloudflare:speedtest.speed1" # doesn't have standard columns
)

readonly COMMON_FLAGS=(
	-verbose
	-local
	-gcs-bucket pusher-mlab-sandbox
	-experiment ndt
)

main() {
	local table
	local project_id
	local dataset_table
	local dataset
	local datatype

	execute go build -race -o jostler ../cmd/jostler
	for table in "${BQ_TABLES[@]}"; do
		project_id="${table%%:*}"
		dataset_table="${table#*:}"
		dataset="${dataset_table%%.*}"
		datatype="${table##*.}"

		if create_datatype_schema "${project_id}" "${dataset}" "${datatype}"; then
			verify_datatype_schema "${table}"
			execute ./jostler "${COMMON_FLAGS[@]}" -datatype "${datatype}" -datatype-schema-file "${datatype}:${table}.datatype"
			verify_table_schema "${USER}" "${datatype}"
		fi
		printf '=%.0s' {1..72}
		echo
	done
}

# Create a datatype schema file if one doesn't already exist or if the
# user wants to recreate it anyway.
create_datatype_schema() {
	local project_id="$1"
	local dataset="$2"
	local datatype="$3"
	
	# Does the datatype schema file already exist?
	if [[ -f "${table}.datatype" ]]; then
		local answer="n"
		read -r -p "Do you want to use ${table}.datatype in the current directory [y/N]? " answer
		if [[ "${answer,,}" == "y" ]]; then
			return 0
		fi
	fi

	# Does this table exist in BigQuery?
	echo bq show --project_id "${project_id}" "${dataset}.${datatype}"
	if ! bq show --project_id "${project_id}" "${dataset}.${datatype}" >/dev/null 2>/dev/null; then
		echo "${project_id}:${table}" does not exist
		read -r -p "Hit Enter when ready to skip this table..."
		return 1
	fi
	# Table exists in BigQuery.  Fetch its full schema and extract
	# the "schema" field to create its datatype schema.
	execute bq show --format prettyjson "${table}" > "${table}.full"
	execute jq .schema.fields "${table}.full" | execute tail -n +1 > "${table}.datatype"
	return 0
}

# Prompt the user to verify the datatype schema file does not have
# standard columns.  If it does, they should be manually deleted.
verify_datatype_schema() {
	table="$1"

	cat <<EOF
Edit "${table}.datatype" to remove standard columns if they are present.
A valid datatype file would look like the following (spacing doesn't matter):

[
  { "name": "Field1", "type": "INTEGER" },
  { "name": "Field2", "type": "FLOAT" },
  { "name": "NMSVersion", "type": "STRING" },
  { "name": "UUID", "type": "STRING" }
] 

EOF
	read -r -p "Hit Enter when ready..."
}

# Verify the table schema jostler created is valid and BigQuery can
# create a table with it.  If the table already exists, it must be
# first deleted.
#
# Note that table creation and deletion is done only in the user's
# dataset in the mlab-sandbox project.
verify_table_schema() {
	local dataset="$1"
	local datatype="$2"

	if [[ "${dataset}" != "${USER}" ]]; then
		echo table creation and deletion is only allowed is the user\'s dataset
		return
	fi

	# Delete the table for this datatype if it already exists.
	if bq show --project_id "${PROJECT_ID}" "${dataset}.${datatype}" >/dev/null 2>/dev/null; then
		echo Deleting the existing "${PROJECT_ID}:${dataset}.${datatype}" table
		# Delete the table (bq rm prompts the user to verify).
		execute bq rm --project_id "${PROJECT_ID}" --table "${dataset}.${datatype}"
	fi

	# Create a table with the schema that jostler saved.
	execute bq mk --project_id "${PROJECT_ID}" --table "${dataset}.${datatype}" "${datatype}-table.json"

	# Show the newly created table's full schema.
	#execute bq show --project_id "${PROJECT_ID}" --format prettyjson "${dataset}.${datatype}"

	# Ask the user if they want to delete the newly created table
	# (bq rm prompts the user to verify).
	execute bq rm --project_id "${PROJECT_ID}" --table "${dataset}.${datatype}"
}

execute() {
	(>&2 echo "$@")
	"$@"
	(>&2 echo)
}

main "$@"
