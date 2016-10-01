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


PACKAGE=emma-quickstart

mvn archetype:generate                   \
  -DarchetypeGroupId=eu.stratosphere     \
  -DarchetypeArtifactId=emma-quickstart  \
  -DarchetypeVersion=1.0-SNAPSHOT        \
  -DgroupId=org.myorg.quickstart         \
  -DartifactId=$PACKAGE                  \
  -Dversion=0.1                          \
  -Dpackage=org.emma.quickstart          \
  -DinteractiveMode=false

#
# Give some guidance
#
echo -e "\\n\\n"
echo -e "\\tA sample quickstart Emma project has been created."
echo -e "\\tSwitch into the directory using"
echo -e "\\t\\t cd $PACKAGE"
echo -e "\\tImport the project there using your favorite IDE (Import it as a maven project)"
echo -e "\\tBuild a jar inside the directory using"
echo -e "\\t\\t mvn clean package <profile> where available prlofiles are -Pflink and -Pspark"
echo -e "\\tYou will find the runnable jar in $PACKAGE/target"
echo -e "\\n\\n"
