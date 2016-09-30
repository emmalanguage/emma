#!/bin/bash

files=$(pcregrep -lM "package eu.stratosphere.emma\r?\n?package" $(find -name "*.scala" |grep -v BaseCompilerSpec))

for f in $files; do
  echo "Processing file $f"

  sed -i 's/package eu.stratosphere.emma/package org.emmalanguage/' $f
  sed -i "s/private\[emma\]/private\[emmalanguage\]/" $f
  sed -i "s/protected\[emma\]/protected\[emmalanguage\]/" $f
  
  target=$(echo $f |sed 's/eu\/stratosphere\/emma/org\/emmalanguage/')
  mkdir -p $(dirname $target)  # git mv only works if the target dir exists
  git mv $f $target
done
