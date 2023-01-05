package api

// StandardColumnsV0 defines version 0 of the standard columns included
// in every line (row) along with the raw data from the measurement service.
//
// Note that the Raw field is a placeholder for the actual measurement
// data in JSON format produced by the measurement service.  Ideally, its
// type should be declared as "any" but bigquery.InferSchema() does not
// support "any" (or "interface{}").
type StandardColumnsV0 struct {
	Archiver ArchiverV0 `bigquery:"archiver"` // archiver details
	Raw      string     `bigquery:"raw"`      // measurement data (file contents) in JSON format
}

// ArchiverV0 defines version of 0 of archiver details that includes:
// 1- The exact version of the running instance of the program.
// 2- Where the JSONL bundle is archived and which files it includes.
type ArchiverV0 struct {
	Version    string `bigquery:"Version"`    // running version of this program
	GitCommit  string `bigquery:"GitCommit"`  // git commit sha1 of this program
	ArchiveURL string `bigquery:"ArchiveURL"` // GCS object name of the bundle
	Filename   string `bigquery:"Filename"`   // pathname of the file in the bundle
}
