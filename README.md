# Emma

Emma is a quotation-based DSL for scalable data analysis embedded in Scala.

Our goal is to improve developer productivity by hiding parallelism aspects behind a high-level, 
declarative API and through deep reuse of native Scala syntax and constructs.

Emma supports state-of-the-art dataflow engines such as 
[Apache Flink](https://flink.apache.org/) and 
[Apache Spark](https://spark.apache.org/) as backend co-processors.

## Features

DSLs for scalable data analysis are embedded through types.
In contrast, Emma is *based on quotations* (similar to [Quill](http://getquill.io/)).
Data analysis pipelines assembled in Emma are *quoted* in a special function calls.
This approach has two benefits.

First, it allows to re-use native Scala syntax such as
[`for`-comprehensions](http://docs.scala-lang.org/tutorials/FAQ/yield.html),
[case-classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html), and 
[pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html) in the quoted DSL.

Second, it allows to *analyze and optimize* the quoted data analysis pipelines holistically 
and off-load data-parallel terms on a co-processor engine such as Apache Flink or Apache Spark.

## Examples

The [emma-examples](emma-examples/emma-examples-library/src/main/scala/org/emmalanguage/examples) module contains examples from various fields.

- [Graph Analysis](emma-examples/emma-examples-library/src/main/scala/org/emmalanguage/examples/graphs)
- [Machine Learning](emma-examples/emma-examples-library/src/main/scala/org/emmalanguage/examples/ml)

## Learn More

Check [emma-language.org](http://emma-language.org) for further information.

## Build

- JDK 7+ (preferably JDK 8)
- Maven 3

Run

```bash 
$ mvn clean package -DskipTests
```

to build *Emma* without running any tests. 

For more advanced build options including integration tests for the target runtimes please see the ["Building Emma" section in the Wiki](../../wiki/Building-Emma).
