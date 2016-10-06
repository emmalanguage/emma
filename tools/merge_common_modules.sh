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

# move all files from `emma-backend/src` to `emma-language/src`
if [[ -e emma-backend/src ]]; then
  for f in $(cd emma-backend/src && find * -type f); do
    if [[ ! -e emma-language/src/$(dirname $f) ]]; then
      mkdir -p emma-language/src/$(dirname $f)
    fi
    git mv emma-backend/src/$f emma-language/src/$f
  done
fi

# move all files from `emma-common-macros/src` to `emma-language/src`
if [[ -e emma-common-macros/src ]]; then
  for f in $(cd emma-common-macros/src && find * -type f); do
    if [[ ! -e emma-language/src/$(dirname $f) ]]; then
      mkdir -p emma-language/src/$(dirname $f)
    fi
    git mv emma-common-macros/src/$f emma-language/src/$f
  done
fi

# move all files from `emma-common/src` to `emma-language/src`
if [[ -e emma-common/src ]]; then
  for f in $(cd emma-common/src && find * -type f); do
    if [[ ! -e emma-language/src/$(dirname $f) ]]; then
      mkdir -p emma-language/src/$(dirname $f)
    fi
    git mv emma-common/src/$f emma-language/src/$f
  done
fi
