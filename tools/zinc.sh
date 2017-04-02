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

STARTSTOP=$1

# zinc options
ZN_ROOT="$HOME/.zinc"
ZN_HOME="$ZN_ROOT/zinc-0.3.13"
ZN_PORT=3030
ZN_SCALA_ROOT="$HOME/.m2/repository/org/scala-lang"
ZN_SCALA_COMPILER="$ZN_SCALA_ROOT/scala-compiler/2.11.8/scala-compiler-2.11.8.jar"
ZN_SCALA_LIBRARY="$ZN_SCALA_ROOT/scala-library/2.11.8/scala-library-2.11.8.jar"
ZN_SCALA_REFLECT="$ZN_SCALA_ROOT/scala-reflect/2.11.8/scala-reflect-2.11.8.jar"
ZN_SCALA_PATH="$ZN_SCALA_COMPILER,$ZN_SCALA_LIBRARY,$ZN_SCALA_REFLECT"

case $STARTSTOP in

    (start)
        $ZN_HOME/bin/zinc -scala-path=$ZN_SCALA_PATH -port=$ZN_PORT -start
    ;;

    (stop)
        $ZN_HOME/bin/zinc -shutdown
    ;;

    (status)
        $ZN_HOME/bin/zinc -status
    ;;

    (*)
        $ZN_HOME/bin/zinc $@
    ;;

esac
