#!/usr/bin/env bash

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
