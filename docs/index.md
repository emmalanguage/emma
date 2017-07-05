---
layout: simple
title: Homepage
---

<div class="index-header">
  <h1>Meet Emma</h1>
  <p><strong>Emma</strong> is a quotation-based Scala DSL for scalable data analysis.</p>
  <p>
    Emma supports state-of-the-art dataflow engines such as 
    <a href="https://flink.apache.org/">Apache Flink</a> and 
    <a href="https://spark.apache.org/">Apache Spark</a>.
  </p>
</div>

## Main Features

DSLs for scalable data analysis are embedded through types.
In contrast, Emma is *based on quotations* (similar to [Quill](http://getquill.io/)).
This approach has several benefits which directly affect developer productivity.

First, it allows to reuse Scala-native, declarative constructs in the DSL.
Quoted Scala syntax such as 
[`for`-comprehensions](http://docs.scala-lang.org/tutorials/FAQ/yield.html),
[case-classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html), and 
[pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html) 
are thereby lifted to an intermediate representation called *Emma Core*.

Second, it allows to *analyze and optimize* Emma Core terms holistically. 
Subterms of type `DataBag[A]` are thereby transformed and off-loaded to a parallel dataflow engine such as Apache Flink or Apache Spark.

## Learn More

For **a discussion of the benefits of Emma vs Flink and Spark APIs**, check the [Meet Emma](https://docs.google.com/presentation/d/1IM6VhGGg--dx5dEnCJtWkD0JXCl9Sw-wzTgr3Cj6uig/edit?usp=sharing) presentation and the [emma-tutorial](https://github.com/emmalanguage/emma-tutorial).

For **a brief introduction to the core API** and its most distinctive features, check the [Programming Guide]({{ site.baseurl }}/programming-guide.html).

For **instructions on setting up an Emma-based project**, check the [Project Setup]({{ site.baseurl }}/project-setup.html).

To learn about **Emma internals**, check the [Emma Wiki](https://github.com/emmalanguage/emma/wiki).

If you discuss this project in a research publication, please cite our SIGMOD 2015 paper
["Implicit Parallelism through Deep Language Embedding"](http://aalexandrov.name/assets/pdf/2015.SIGMOD.ImplicitParallelism.pdf).
