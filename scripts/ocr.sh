#!/bin/bash

# deal with spaces in file names
IFS=$(echo -en "\n\b")

tiff_folder=/tmp/tiff

mkdir -p $tiff_folder

pdf=$1
txt=$2

bn=$(basename $txt .txt)

echo ocring $pdf to $txt

convert -alpha deactivate -density 300 $pdf -depth 8  $tiff_folder/$bn-%04d.tiff
>$txt
for tiff in $tiff_folder/$bn*.tiff
do
    tesseract ${tiff} stdout >> $txt
done
