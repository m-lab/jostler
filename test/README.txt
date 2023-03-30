There are three files in this test directory to help with integration
and e2e testing.

1. schema.sh is a bash script that can be used for testing table schema
   creation from datatype schemas.  Read the comments at the beginning of
   schema.sh to understand what it does and how it works.

2. cmd/gen_data/gen_data.go is a very simple Go program that mimics a
   measurement service by creating measurement data files for jostler
   to bundle and upload.

3. e2e.sh is a bash script to invoke jostler with the right parameters
   for e2e testing.

4. cmd/check_bundles/check_bundles.go is a Go program that validates
   data and index bundles after e2e tests as follows:
   - For every index bundle there is a data bundle and vice versa.
   - Every file specified in the index bundle exists in the data
     bundle and vice versa.
   - The order in which files appear in the index and data bundles
     are the same.

The easiest way to do e2e testing is to run jostler in one terminal and
run gen_data.go in another terminal as shown below:


[Term A]

$ cd /path/to/your/jostler/directory
$ EXPERIMENT=experiment DATATYPE=datatype1 ./test/e2e.sh  # add -c to run in a container
# wait until gen_data.go (in Term B) has been killed and then kill jostler (^C)

Because e2e.sh invokes jostler with the -gcs-local-disk flag, jostler will
use testhelper's local disk storage implementation which mimics downloads
from and uploads to cloud storage (GCS).  This makes testing and debugging
a lot easier.


[Term B]
$ cd /path/to/your/jostler/directory/test
$ EXPERIMENT=experiment DATATYPE=datatype1 go run cmd/gen_data/gen_data.go -sleep 1s -verbose
# wait a minute or so and then kill the command (^C)


[Term C]
$ cd /path/to/your/jostler/directory
$ while :; do tree e2e; sleep 3; done

You will see that data files are created by gen_data.go (in Term B)
in the subdirectories of:

	e2e/local/var/spool/$EXPERIMENT/$DATATYPE/<yyyy>/<mm>/<dd>

and are deleted by jostler after data and index bundles are "uploaded" to the
following directory:

	e2e/gcs/autoload/v1/$EXPERIMENT/$DATAYPE/<yyyy>/<mm>/<dd>

If you'd like, you can run check_bundles to verify the correctness of
data and index bundles as follows:

$ cd test
$ go run cmd/check_bundles/check_bundles.go -verbose ../e2e/gcs/autoload/v1/experiment
