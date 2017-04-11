---
layout: simple
title: Emma Project Setup
---

# Emma Project Setup

Make sure that Sonatype snapshots are enabled in your `~/.m2/settings.xml`.

```xml
<profiles>
  <profile>
    <id>allow-snapshots</id>
    <activation><activeByDefault>true</activeByDefault></activation>
    <repositories>
      <repository>
        <id>sonatype.snapshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
      </repository>
      <!-- more repositories here -->
    </repositories>
  </profile>
</profiles>
```

## New Project

To bootstrap a new project `org.acme:emma-quickstart` from a Maven archetype, use the following command.

```bash
mvn archetype:generate -B                  \
    -DartifactId=emma-quickstart           \
    -DgroupId=org.acme                     \
    -Dpackage=org.acme.emma                \
    -DarchetypeArtifactId=emma-quickstart  \
    -DarchetypeGroupId=org.emmalanguage    \
    -DarchetypeVersion=0.2-SNAPSHOT
```

## Existing Project

To add Emma to an existing project, add the `emma-language` dependency

```xml
<!-- Core Emma API and compiler infrastructure -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-language</artifactId>
    <version>0.2-SNAPSHOT</version>
</dependency>
```

and either `emma-flink` or `emma-spark` depending on the desired backend.

```xml
<!-- Emma backend for Flink -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-flink</artifactId>
    <version>0.2-SNAPSHOT</version>
</dependency>
```

```xml
<!-- Emma backend for Spark -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-spark</artifactId>
    <version>0.2-SNAPSHOT</version>
</dependency>
```
