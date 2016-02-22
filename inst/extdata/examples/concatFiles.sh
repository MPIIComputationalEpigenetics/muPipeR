#!/bin/sh
# Concatenates input files into one output files using a separation line
# $1: output file
# $... input files
OUTFILE=$1
touch ${OUTFILE}

for i in ${@:2}
do
	echo "################################################################################" >> $1
    cat $i >> $1
done
