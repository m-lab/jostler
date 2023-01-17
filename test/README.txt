There are three files in this test directory to help with integration
and e2e testing.

1. schema.sh is a bash script that can be used for testing table schema
   creation from datatype schemas.  Read the comments at the beginning of
   schema.sh to understand what it does and how it works.

2. data.go is a very simple Go program that mimics a measurement service
   by creating measurement data files for jostler to bundle and upload.

3. e2e.sh is a bash script to invoke jostler with the right parameters
   for e2e testing.

The easiest way to do e2e testing is to run jostler in one terminal and
run data.go in another terminal as shown below:


[Term A]
$ cd /path/to/your/jostler/directory
$ EXPERIMENT=experiment DATATYPE=datatype1 ./test/e2e.sh

Because e2e.sh invokes jostler with the -local-disk flag, jostler will
use testhelper's local disk storage implementation which mimics downloads
from and uploads to cloud storage (GCS).  This makes testing and debugging
a lot easier.


[Term B]
$ cd /path/to/your/jostler/directory/test
$ EXPERIMENT=experiment DATATYPE=datatype1 go run data.go -sleep 1s -verbose


[Term C]
$ cd /path/to/your/jostler/directory
$ while :; do tree e2e; sleep 3; done

You will see that data files are created by test/data.go in the
subdirectories of:

	e2e/local/var/spool/$EXPERIMENT/$DATATYPE/<yyyy>/<mm>/<dd>

and are deleted by jostler after bundles and their indices are "uploaded" to the
following directory:

	e2e/gcs/autoload/v1/$EXPERIMENT/$DATAYPE/date=<yyyy>-<mm>-<dd>
