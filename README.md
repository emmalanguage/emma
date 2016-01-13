# Emma

*Emma* is a Scala API for scalable data analysis. The goal of *Emma* is to improve developer productivity by hiding parallelism aspects behind a high-level, declarative API. *Emma* currently creates code for [Apache Flink](https://flink.apache.org/) and [Apache Spark](https://spark.apache.org/).

More information about the project is available at [http://emma-language.org](http://emma-language.org).

## Features

Programs written for distributed execution engines usually suffer from some well-known pitfalls: 

1. Details and subtleties of the target engine **execution model** and API must be well understood to write efficient programs. 
2. The program code can be **hard to read**, due to API and specifics of the target engine. 
3. Since algorithms on the specific engine APIs are usually implemented in an explicit fashion, **opportunities for optimization** are missed out. 

*Emma* offers a declarative API for parallel collection processing. *Emma* programs can benefit from **deep linguistic re-use** of native Scala features like `for`-comprehensions, case-classes and pattern matching. Data analysis algorithms are **holistically optimized** for data parallel execution on a co-processor engine like *Flink* or *Spark* in a macro-based compiler pipeline.  

### Core API

Importing *Emma*

```scala 
import eu.stratosphere.emma.api._
```

The following examples assume the following domain schema: 

```scala
case class Person(id: Long, email: String, name: String)
case class Email(id: Long, from: String, to: String, msg: String)
```

#### Primitive Type: `DataBag[A]`

Parallel computation in *Emma* is represented by expressions over a core `DataBag[A]` type (which models a collection with duplicated and without a particular order). 

`DataBag[A]`s are created either using a Scala `Seq[A]` or the `read` method and a `CSVInputFormat[A]`. 

```scala
val squares = DataBag(1 to 42 map { x => (x, x * x) })              // DataBag[(Int, Int)]
val emails  = read("hdfs://emails.csv", new CSVInputFormat[Email]); // DataBag[Email]
```

A `DataBag[A]` can be converted to a (local) `Seq[A]` using the `fetch` method or written to disk using `write` and a `CSVOutputFormat[A]`. 

```scala
emails.fetch() // Seq[Email]
write("hdfs://squares.csv", new CSVOutputFormat[(Int, Int)])(squares)
```

#### Primitive Computations: `fold`s

The core processing abstraction provided by `DataBag[A]` is a generic pattern for parallel collection processing called `fold`. 

To understand how `fold` works, assume that a `DataBag[A]` instance can be constructed in one of three ways: As the empty bag, a singleton bag, or the union of two existing bags. The `fold` method conceptually works by (1) systematically deconstructing a `DataBag[A]` instance, (2) replacing the constructors with corresponding user-defined functions, and (3) evaluating the resulting expression.

```scala
def fold[B](e: B)(s: A => B, u: (B, B) => B): B = this match {
   case Empty         => e
   case Sng(x)        => s(x)
   case Union(xs, ys) => u(xs.fold(e)(s, u), ys.fold(e)(s, u))
}
```

where `e` substitutes `Empty` bag, `s` the singleton bag, and `u` the desired bag union operation.

Values collection processing primitives can be specified as a `fold`. 

```scala 
val min = xs.fold(Int.MaxValue)(identity, Math.min(_,_))
val max = xs.fold(Int.MinValue)(identity, Math.max(_,_))
val sum = xs.fold(0, x => 1, _ + _)
```

*Emma* offers pre-defined aliases for common `fold` operators:

Fold Alias                     | Purpose
-------------------------------|-----------------------------
`min`, `max`, `sum`, `count` … | Aggregation
`exists`, `forall`, …          | Existential Qualifiers

#### Declarative Data-flows

Joins and cross products in *Emma* can be declared using `for`-comprehension syntax in a manner akin to *Select-From-Where* expressions known from SQL. 

```scala
for {
  email <- emails
  from  <- people
  to    <- people
  if from.email == email.from 
  if to.email == email.to
} yield(from, to, email)
```

In addition *Emma* offers the following bag combinators:

```scala
val csx = DataBag(Seq("Meijer", "Beckmann", "Wadler"))     // computer scientists
val fbx = DataBag(Seq("Meijer", "Pelé", "Meijer"))  // footballers

// union (with bag semantics)
csx plus fbx 
// DataBag(Seq("Meijer", "Beckmann", "Pelé", "Wadler", "Meijer", "Meijer"))

// difference (with bag semantics)
fbx minus csx
// DataBag(Seq("Pelé", "Meijer"))

// duplicate elimination
fbx distinct 
// DataBag(Seq("Meijer", "Pelé"))
```

#### Nesting

In addition to the collection manipulation primitives presented above, *Emma* offers a `groupBy` combinator, which (conceptually) groups bag elements by key and introduces a level of nesting. 

To compute the average size of E-Mail messages by sender, for example, one would write the following expression. 

```scala 
for {
    (sender, mails) <- emails.groupBy(_.from) 
} yield {
    val sum = mails map { _.msg.length } sum()
    val cnt = mails.count()
    sum / cnt
}
```

#### The Macro-Based Compiler

The presented API is not abstract: The semantics of each operator are given directly by the corresponding [`DataBag[A]`-method definitions](./blob/master/emma-common/src/main/scala/eu/stratosphere/emma/api/DataBag.scala). This allows you to incrementally develop, test, and debug data analysis algorithms at small scale locally as a pure Scala programs.

```scala 
val words = for {
    line <- read(inPath, new TextInputFormat[String]('\n'))
    word <- DataBag[String](line.toLowerCase.split("\\W+"))
} yield word

// group the words by their identity and count the occurrence of each word
val counts = for {
    group <- words.groupBy[String] { identity }
} yield (group.key, group.values.size)

// write the results into a CSV file
write(outPath, new CSVOutputFormat[(String, Long)])(counts)
```

Once the algorithm is ready, simply wrap the code in the `emma.parallelize` macro which modifies the code and wraps it in an `Algorithm` object. The returned object can be executed on an engine like *Flink* or *Spark* (for scalable data-parallel execution) or the `Native` runtime (which will run the quoted code unmodified). 

```scala
val algorithm = emma.parallelize {
  // word count code from above goes here!
}

// execute the algorithm on a parallel execution engine
algorithm.run(runtime.factory("flink"))
```

The `emma.parallelize` macro identifies all `DataBag[A]` terms in the quoted code fragment and re-writes them jointly in order to maximize the degree of parallelism. For example, the `groupBy` and the subsequent `fold`s from the [nesting](#nesting) example are executed using more efficient target-runtime primitives like `reduceByKey`. The holistic translation approach also allows us to transparently insert primitives like `cache` and `broadcast` and `partitionBy` based on static control and data-flow analysis of the quoted code. 

For more information on the *Emma* compile pipeline see our recent SIGMOD publication [Implicit Parallelism through Deep Language Embedding](http://aalexandrov.name/assets/pdf/2015.SIGMOD.ImplicitParallelism.pdf).

## Getting Started

### Dependencies

* JDK 7+ (preferably JDK 8)
* Maven 3

### Build using Maven

Add the *Emma* dependency

```xml
<!-- Basic Emma API (required) -->
<dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>emma-language</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Optionally add the *Flink* or *Spark* back-end(s).

```xml
<!-- Emma backend for Flink (optional) -->
<dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>emma-flink</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

```xml
<!-- Emma backend for Spark (optional) -->
<dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>emma-spark</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Run

```bash 
$ mvn clean package -DskipTests
```

to build *Emma* without running any tests. 

For more advanced build options including integration tests for the target runtimes please see https://github.com/stratosphere/emma/wiki/Building-Emma . 

## Examples

The [emma-examples](https://github.com/stratosphere/emma/tree/master/emma-examples/src/main/scala/eu/stratosphere/emma/examples) module contains multiple examples from various fields:

- Data Mining
- Databases (including TCP-H)
- Graph Analysis

