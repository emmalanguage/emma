# Emma

*A quotation-based Scala DSL for scalable data analysis.*

[![Build Status](https://travis-ci.org/emmalanguage/emma.svg?branch=master)](https://travis-ci.org/emmalanguage/emma)

## Goals

Our goal is to improve developer productivity by hiding parallelism aspects behind a high-level, 
declarative API which maximises reuse of native Scala syntax and constructs.

Emma supports state-of-the-art dataflow engines such as 
[Apache Flink](https://flink.apache.org/) and 
[Apache Spark](https://spark.apache.org/) as backend co-processors.

## Features

DSLs for scalable data analysis are embedded through types.
In contrast, Emma is *based on quotations* (similar to [Quill](http://getquill.io/)).
This approach has two benefits.

First, it allows to reuse Scala-native, declarative constructs in the DSL.
Quoted Scala syntax such as 
[`for`-comprehensions](http://docs.scala-lang.org/tutorials/FAQ/yield.html),
[case-classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html), and 
[pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html) 
are thereby lifted to an intermediate representation called *Emma Core*.

Second, it allows to *analyze and optimize* Emma Core terms holistically. 
Subterms of type `DataBag[A]` are thereby transformed and off-loaded to a parallel dataflow engine such as Apache Flink or Apache Spark.

## Examples

The [emma-examples](emma-examples/emma-examples-library/src/main/scala/org/emmalanguage/examples) module contains examples from various fields.

- Graph Analysis
  - [Connected Components](emma-examples/src/main/scala/org/emmalanguage/examples/graphs/ConnectedComponents.scala)
  - [Triangle Enumeration](emma-examples/src/main/scala/org/emmalanguage/examples/graphs/EnumerateTriangles.scala)
  - [Transitive Closure](emma-lib/src/main/scala/org/emmalanguage/lib/graphs/transitiveClosure.scala)
- Supervised Learning
  - [Naive Bayses Classification](emma-lib/src/main/scala/org/emmalanguage/lib/ml/classification/naiveBayes.scala)
- Unsupervised Learning
  - [k-Means Clustering](emma-lib/src/main/scala/org/emmalanguage/lib/ml/clustering/kMeans.scala)
- Text Processing
  - [Word Count](emma-examples/src/main/scala/org/emmalanguage/examples/text/WordCount.scala)

## Learn More

Check [emma-language.org](http://emma-language.org) for further information.

## Build

- JDK 7+ (preferably JDK 8)
- Maven 3

Run

```bash
mvn clean package -DskipTests
```

to build Emma without running any tests. 

For more advanced build options including integration tests for the target runtimes please see the ["Building Emma" section in the Wiki](../../wiki/Building-Emma).
