#!/bin/sh
# Appends a string as line to a file
# $1: input file
# $2: output file
# $3: string
cp $1 $2
echo $3 >> $2
