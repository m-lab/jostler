package api

// IndexV1 defines individual entries in the index bundle that describes a
// data bundle.  The index bundle is uploaded to GCS as datatype of index1
// so the autoload agent in the pipeline does not have to distinguish
// between measurement data bundles and index bundles.  In other words,
// as far as the pipeline is concerned, index1 is just another datatype.
// Index bundles have the same name as the bundle they describe but their
// extension is ".index".
type IndexV1 struct {
	Filename  string // full pathname to the measurement data file
	Size      int    // size of the measurement data file
	TimeAdded string // when measurement data file was added to data bundle
}
