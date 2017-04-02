#!/usr/bin/env bash
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

ZN_ROOT="$HOME/.zinc"
ZN_HOME="$ZN_ROOT/zinc-0.3.13"
ZN_HASH="8cad8cedb59b9b3b7cd92f95f2c81b91"
ZN_LINK="http://downloads.typesafe.com/zinc/0.3.13/zinc-0.3.13.tgz"

FILE_PATH="${BASH_SOURCE%/*}"

if [[ ! -d "$FILE_PATH" ]]; then
    FILE_PATH="$PWD"
fi

# create "$HOME/.zinc.sh" folder it does not exist
if [ ! -e $ZN_ROOT ]; then
    mkdir $ZN_ROOT
fi

# ensure that "$HOME/.zinc.sh" is a writable folder
if [ ! -d $ZN_ROOT ] || [ ! -x $ZN_ROOT ] || [ ! -w $ZN_ROOT ]; then
    echo "$ZN_ROOT is not a writable folder" 1>&2
    exit 1
fi

if [ -e $ZN_HOME ]; then
    if [ ! -d $ZN_HOME ] || [ ! -x $ZN_HOME ] || [ ! -w $ZN_HOME ]; then
        echo "$ZN_HOME is not a writable folder" 1>&2
        exit 1
    fi

    if [ $ZN_HASH != $(cd $ZN_HOME && find . -type f -exec md5sum {} \; | sort | md5sum | cut -d ' ' -f 1) ]; then
        echo "md5sum of '$ZN_HOME' does not match expected value '$ZN_HASH'"
        echo "removing '$ZN_HOME'"
        rm -Rf $ZN_HOME
        echo "downloading '$ZN_LINK' in '$ZN_HOME'"
        curl -s $ZN_LINK | tar xz -C $ZN_ROOT
    else
        echo "md5sum of existing folder '$ZN_HOME' matches expected value '$ZN_HASH'"
    fi
else
    echo "downloading '$ZN_LINK' in '$ZN_HOME'"
    curl -s $ZN_LINK | tar xz -C $ZN_ROOT
fi

test $ZN_HASH == $(cd $ZN_HOME && find . -type f -exec md5sum {} \; | sort | md5sum | cut -d ' ' -f 1)
