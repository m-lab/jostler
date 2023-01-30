package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var (
	localDataDir = flag.String("local-data-dir", "../e2e/local/var/spool", "local pathname under which measurement data is created")
	experiment   = flag.String("experiment", "", "name of the experiment")
	datatype     = flag.String("datatype", "", "name of the datatype")
	nDays        = flag.Int("days", 7, "number of date subdirectories")
	sleep        = flag.Duration("sleep", 100*time.Millisecond, "sleep time in milliseconds between file creations")
	verbose      = flag.Bool("verbose", false, "enable verbose mode")

	dirs = []string{}
)

func main() {
	flag.Parse()
	if *experiment == "" {
		*experiment = os.Getenv("EXPERIMENT")
	}
	if *datatype == "" {
		*datatype = os.Getenv("DATATYPE")
	}

	if *experiment == "" || *datatype == "" {
		fmt.Println("must specify both experiment and datatype") //nolint
		os.Exit(1)
	}
	createDateSubdirs()
	createDataFiles()
}

func createDateSubdirs() {
	now := time.Now()
	for i := 0; i < *nDays; i++ {
		dir := fmt.Sprintf("%s/%s/%s/%s", *localDataDir, *experiment, *datatype, now.AddDate(0, 0, -i).Format("2006/01/02"))
		if *verbose {
			fmt.Printf("creating %s\n", dir) //nolint
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			panic(err)
		}
		dirs = append(dirs, dir)
	}
}

func createDataFiles() {
	if len(dirs) == 0 {
		return
	}
	rand.Seed(int64(os.Getpid()))
	n := 0
	for {
		// Favor today.
		index := rand.Intn(len(dirs) + 21) //nolint
		if index >= len(dirs) {
			index = 0
		}
		createDataFile(index)
		time.Sleep(*sleep)
		fmt.Printf("%v\r", n) //nolint
		n++
	}
}

func createDataFile(index int) {
	content := `{"UUID":"1234","ToolVersion":"0.1.2","Result":100}`
	now := time.Now().UTC().Format("T150405.000000Z")
	filename := fmt.Sprintf("%s/%s.json", dirs[index], now)
	if *verbose {
		fmt.Printf("creating %v\n", filename) //nolint
	}
	if err := os.WriteFile(filename, []byte(content), 0o666); err != nil {
		panic(err)
	}
}
