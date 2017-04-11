---
layout: simple
title: Homepage
---

# Hello, Emma!

**Emma** is a quotation-based DSL for scalable data analysis embedded in Scala.

Our goal is to improve developer productivity by hiding parallelism aspects behind a high-level, 
declarative API and through deep reuse of native Scala syntax and constructs.

Emma supports state-of-the-art dataflow engines such as 
[Apache Flink](https://flink.apache.org/) and 
[Apache Spark](https://spark.apache.org/) as backend co-processors. 

## Main Features

DSLs for scalable data analysis are embedded through types.
In contrast, Emma is *based on quotations* (similar to [Quill](http://getquill.io/)).
Data analysis pipelines assembled in Emma are *quoted* in a special function calls.
This approach has two benefits.
First, it allows to re-use native Scala syntax such as
[`for`-comprehensions](http://docs.scala-lang.org/tutorials/FAQ/yield.html),
[case-classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html), and 
[pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html) in the quoted DSL code fragments.
Second, it allows to *analyze and optimize* the quoted data analysis pipelines holistically 
and off-load data-parallel terms on a co-processor engine such as Apache Flink or Apache Spark.

## Learn More

For a brief introduction to the core API and its most distinctive features, check the [Programming Guide]({{ site.baseurl }}/programming-guide.html).

For instructions on setting up an Emma-based project, check the [Project Setup]({{ site.baseurl }}/project-setup.html).

For more Emma-related resources, check the [Emma Wiki](https://github.com/emmalanguage/emma/wiki).

If you want to cite this project in a research publication, please use our SIGMOD 2015 paper
[Implicit Parallelism through Deep Language Embedding](http://aalexandrov.name/assets/pdf/2015.SIGMOD.ImplicitParallelism.pdf).
