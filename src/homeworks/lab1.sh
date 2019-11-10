#!/bin/sh

# part 1
cd ../mapreduce
go test -run Sequential

# part 2
cd ../main
sh ./test-wc.sh

# part 3
cd ../mapreduce
go test -race -run TestParallel

# part 4
go test -run Failure

# part 5
cd ../main
go run ii.go master sequential pg-*.txt
sh ./test-ii.sh
