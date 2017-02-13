# Emma

*Emma* is a Scala DSL for scalable data analysis.
Our goal is to improve developer productivity
by hiding parallelism aspects behind a high-level, declarative API
and through deep reuse of native Scala syntax and constructs.
*Emma* supports state-of-the-art dataflow engines such as
[Apache Flink](https://flink.apache.org/)
and [Apache Spark](https://spark.apache.org/)
as backend co-processors.

## Features

Programs written for distributed execution engines usually suffer from some well-known pitfalls: 

1. Details and subtleties of the target engine **execution model** must be well understood to write efficient programs. 
2. Due to the number of [abstraction leaks](https://en.wikipedia.org/wiki/Leaky_abstraction), program code can be **hard to read**. 
3. **Opportunities for optimization** are missed out due to hard-coded execution strategies or isolated (per-dataflow) compilation. 

*Emma* offers a declarative API for parallel collection processing.
*Emma* programs benefit from **deep linguistic re-use** of native Scala features such as
[`for`-comprehensions](http://docs.scala-lang.org/tutorials/FAQ/yield.html),
[case-classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html),
and [pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html).
Data analysis algorithms written in *Emma* are **analyzed and optimized holistically**
for data-parallel execution on a co-processor engine such as *Flink* or *Spark*.  

### Core API

*Emma* programs require the following import.

```scala 
import org.emmalanguage.api._
```

The examples below assume the following domain schema: 

```scala
case class Person(id: Long, email: String, name: String)
case class Email(id: Long, from: String, to: String, msg: String)
case class Movie(id: Long, year: Int, title: String)
```

#### Primitive Type: `DataBag[A]`

Parallel computation in *Emma* is represented by expressions over a core type `DataBag[A]`
which models a distributed collection of elements of type `A`
that do not have particular order and may contain duplicates.

`DataBag[A]` instances are created directly from a Scala `Seq[A]`
or by reading from a supported source, e.g. CSV or Parquet.

```scala
val squares = DataBag(1 to 42 map { x => (x, x * x) })                       // DataBag[(Int, Int)]
val emails  = DataBag.readCSV[Email]("hdfs://emails.csv", CSV())             // DataBag[Email]
val movies  = DataBag.readParquet[Movie]("hdfs://movies.parquet", Parquet()) // DataBag[Movie]
```

Conversely, a `DataBag[A]` can be converted back to a `Seq[A]`
or written to a supported sink. 

```scala
emails.fetch()                                            // Seq[Email]
movies.writeCSV("hdfs://movies.csv", CSV())               // Unit
squares.writeParquet("hdfs://squares.parquet", Parquet()) // Unit
```

#### Primitive Computations: `fold`s

The core processing abstraction provided by `DataBag[A]` is a generic pattern for parallel
collection processing called *structural recursion*.

Assume for a moment that `DataBag[A]` instances can be constructed in one of three ways:
the `Empty` bag, a singleton bag `Singleton(x)`, or the union of two existing bags `Union(xs, ys)`.
*Structural recursion* works on bags by

1. systematically deconstructing the input `DataBag[A]` instance, 
2. replacing the constructors with corresponding user-defined functions, and 
3. evaluating the resulting expression.

Formally, the above procedure can be specified as the following second-order function called `fold`.

```scala
def fold[B](e: B)(s: A => B, u: (B, B) => B): B = this match {
  case Empty         => e
  case Singleton(x)  => s(x)
  case Union(xs, ys) => u(xs.fold(e)(s, u), ys.fold(e)(s, u))
}
```

In the above signature, `e` substitutes `Empty`,
`s(x)` substitutes `Singleton(x)`,
and `u(xs, ys)`substitutes `Union(xs, ys)`.

Various collection processing primitives can be specified as a `fold`. 

```scala
val size = xs.fold(0L)(const(1L), _ + _)
val min  = xs.fold(None)(Some(_), lift(_ min _))
val sum  = xs.fold(0)(identity, _ + _)
```

*Emma* offers pre-defined aliases for common `fold` operators:

Fold Alias                          | Purpose
------------------------------------|-----------------------------
`reduce`, `reduceOption`            | General
`isEmpty`, `nonEmpty`, `size`       | Cardinality
`min`, `max`, `top`, `bottom`       | Order-based aggregation
`sum`, `product`                    | Numeric aggregation
`exists`, `forall`, `count`, `find` | Predicate testing
`sample`                            | Random sampling

#### Declarative Dataflows

Joins and cross products in *Emma* can be declared using `for`-comprehension syntax
in a manner akin to *Select-From-Where* expressions known from SQL. 

```scala
for {
  email    <- emails
  sender   <- people
  receiver <- people
  if email.from == sender.email
  if email.to   == receiver.email
} yield (email, sender, receiver)
// DataBag[(Email, Person, Person)]
```

In addition *Emma* offers the following bag combinators:

```scala
val csx = DataBag(Seq("Meijer", "Beckmann", "Wadler")) // computer scientists
val fbx = DataBag(Seq("Meijer", "Pelé", "Meijer"))     // footballers

// union (with bag semantics)
csx union fbx
// res: DataBag(Seq("Meijer", "Beckmann", "Pelé", "Wadler", "Meijer", "Meijer"))

// duplicate elimination
fbx.distinct
// res: DataBag(Seq("Meijer", "Pelé"))
```

#### Nesting

In addition to the collection manipulation primitives presented above,
*Emma* offers a `groupBy` combinator, which (conceptually) groups bag elements by key
and introduces a level of nesting. 

To compute the average size of email messages by sender, for example,
one can write the following straight-forward expression. 

```scala 
for {
  Group(sender, mails) <- emails.groupBy(_.from) 
} yield {
  val sum = mails.map(_.msg.length).sum
  val cnt = mails.size
  sum / cnt
}
```

If the above computation were expressed in a similar way in Flink's or Spark's APIs
it would result in dataflow graphs that do not scale well with the size of the input.
Emma takes a holistic optimization approach which allows to transparently rewrite the program
into scalable Flink/Spark dataflows.

#### DSL Compiler

The presented API is not abstract. The semantics of each operator are given directly by the
default implementation in
[`ScalaSeq[A]`](emma-language/src/main/scala/eu/stratosphere/emma/api/ScalaSeq.scala).
This allows you to incrementally develop, test, and debug data analysis algorithms
at small scale locally as a pure Scala programs.

For example, the following code-snippet that counts words runs out-of-the-box
as a normal Scala program. 

```scala 
val words = for {
  line <- DataBag.readText(input)
  word <- DataBag(line.toLowerCase.split("\\W+"))
} yield word

// group the words by their identity
// and count the occurrences of each word
val counts = for {
  Group(word, occs) <- words.groupBy(identity)
} yield word -> occs.size

// write the results into a CSV file
counts.writeCSV(output, CSV())
```

Once the algorithm is ready, simply wrap it in one of the `emma.onSpark` or `emma.onFlink` macros
depending on the desired co-processor engine for scalable data-parallel execution.

```scala
emma.onSpark {
  // word count code from above goes here!
}

emma.onFlink {
  // word count code from above goes here!
}
```

The backend macros identify all `DataBag[A]` terms in the quoted code fragment
and re-write them jointly in order to maximize the degree of parallelism.
For example, the `groupBy` and the subsequent `fold`s from the [nesting](#nesting) example
are executed using more efficient target-runtime primitives like `reduceByKey`.
The holistic translation approach also allows us to transparently insert co-processor primitives
like `cache`, `broadcast`, and `partitionBy` based on static analysis of the quoted code. 

If you want to cite this project in a research publication, please use our SIGMOD 2015 paper
[Implicit Parallelism through Deep Language Embedding](http://aalexandrov.name/assets/pdf/2015.SIGMOD.ImplicitParallelism.pdf).

For more Emma-related resources, check the
[Emma Wiki](https://github.com/emmalanguage/emma/wiki).

## Getting Started

### Dependencies

* JDK 7+ (preferably JDK 8)
* Maven 3

### Build using Maven

Add the *Emma* dependency

```xml
<!-- Basic Emma API (required) -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-language</artifactId>
    <version>0.2-SNAPSHOT</version>
    <scope>compile</scope>
</dependency>
```

Optionally add either the *Flink* or *Spark* backend.

```xml
<!-- Emma backend for Flink (optional) -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-flink</artifactId>
    <version>0.2-SNAPSHOT</version>
    <scope>runtime</scope>
</dependency>
```

```xml
<!-- Emma backend for Spark (optional) -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-spark</artifactId>
    <version>0.2-SNAPSHOT</version>
    <scope>runtime</scope>
</dependency>
```

Run

```bash 
$ mvn clean package -DskipTests
```

to build *Emma* without running any tests. 

For more advanced build options including integration tests for the target runtimes please see the ["Building Emma" section in the Wiki](../../wiki/Building-Emma). 

## Examples

The [emma-examples](emma-examples/src/main/scala/eu/stratosphere/emma/examples) module contains examples from various fields.

- [Data Mining](emma-examples/src/main/scala/eu/stratosphere/emma/examples/datamining)
- [Relational Databases](emma-examples/src/main/scala/eu/stratosphere/emma/examples/tpch)
- [Graph Analysis](emma-examples/src/main/scala/eu/stratosphere/emma/examples/graphs)
