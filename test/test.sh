#!/bin/bash

set -eu

readonly TABLES=(
	mlab-cloudflare:speedtest.speed2
	mlab-sandbox:ndt.scamper1
)

(cd .. && go build -race -o jostler cmd/jostler/*)

for table in "${TABLES[@]}"; do
	datatype="${table##*.}"

	# fetch the table's full schema
	#bq show --format prettyjson "${table}" > "${table}.full"

	# extract the "schema" field from the table's full schema
	#jq .schema < "${table}.full" | tail -n +1 > "${table}.data"

	# have jostler generate a table schema with standard columns and the data schema
	echo ../jostler -verbose -gcs-bucket mlab-sandbox -schema -datatype "${datatype}" -schema-file "${datatype}:${table}.data"
	../jostler -verbose -gcs-bucket mlab-sandbox -schema -datatype "${datatype}" -schema-file "${datatype}:${table}.data"

	# create a table with the schema that jostler generated
	bq mk --project_id mlab-sandbox --table "${USER}.${datatype}" "${datatype}-schema.json"

	# show the new table's full schema
	bq show --project_id mlab-sandbox --format prettyjson "${USER}.${datatype}"

	# delete the table
	bq rm --project_id mlab-sandbox --table "${USER}.${datatype}"
done

