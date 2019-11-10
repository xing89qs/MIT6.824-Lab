#!/bin/sh

# part 1
cd ../mapreduce
go test -run Sequential

# part 2
cd ../main
sh ./test-wc.sh
