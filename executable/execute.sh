#!/bin/sh

# $1 - sparkSessionName
# $2 - hdfInputCsv
# $3 - hdfsOutputCsvGrouped
# $4 - hdfsOutputCsvUnGrouped

if [ $# -ne 4 ];
then
   echo "**********$0: Missing arguments"
  exit 1
fi

python preprocess.py $1 $2 $3 $4