#!/bin/bash

for i in `seq 100`
do
	go test -race -run Backup
done
