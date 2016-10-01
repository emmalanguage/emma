#!/bin/bash
#
# Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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
