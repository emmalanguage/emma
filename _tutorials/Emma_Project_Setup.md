---
title: "Emma Project Setup"
date: 2015-07-03 11:23:46
---

To start using Emma, follow the [Quickstart](Quickstart.html) or simply add the necessary dependencies to your existing Maven / SBT project:

```xml
<!-- Basic Emma API (required) -->
<dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>emma-language</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
<!-- Emma backend for Flink (optional) -->
<dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>emma-flink</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
<!-- Emma backend for Spark (optional) -->
<dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>emma-spark</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```