# Emma

*Emma* is a Scala DSL for scalable data analysis. Our goal is to improve developer productivity by hiding parallelism aspects behind a high-level, declarative API. *Emma* supports state-of-the-art dataflow engines like [Apache Flink](https://flink.apache.org/) and [Apache Spark](https://spark.apache.org/) as backend co-processors.

More information about the project is available at [http://emma-language.org](http://emma-language.org).

## Features

Programs written for distributed execution engines usually suffer from some well-known pitfalls: 

1. Details and subtleties of the target engine **execution model** must be well understood to write efficient programs. 
2. Due to the number of [abstraction leaks](https://en.wikipedia.org/wiki/Leaky_abstraction), program code can be **hard to read**. 
3. **Opportunities for optimization** are missed out due to hard-coded execution strategies or isolated (per-dataflow) compilation. 

*Emma* offers a declarative API for parallel collection processing. *Emma* programs can benefit from **deep linguistic re-use** of native Scala features like [`for`-comprehensions](http://docs.scala-lang.org/tutorials/FAQ/yield.html), [case-classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html), and [pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html). Data analysis algorithms written in *Emma* are **analyzed and optimized holistically** for data-parallel execution on a co-processor engine like *Flink* or *Spark* in a macro-based compiler pipeline.  

### Core API

*Emma* programs require the following import.

```scala 
import eu.stratosphere.emma.api._
```

The examples below assume the following domain schema: 

```scala
case class Person(id: Long, email: String, name: String)
case class Email(id: Long, from: String, to: String, msg: String)
```

#### Primitive Type: `DataBag[A]`

Parallel computation in *Emma* is represented by expressions over a core type `DataBag[A]`, which modells a collection of elements of type `A` that do not have particular order and may contain duplicates. 

`DataBag[A]` instances are created directly from a Scala `Seq[A]` or by reading from disk. 

```scala
val squares = DataBag(1 to 42 map { x => (x, x * x) })              // DataBag[(Int, Int)]
val emails  = read("hdfs://emails.csv", new CSVInputFormat[Email])  // DataBag[Email]
```

Conversely, a `DataBag[A]` can be converted back to a `Seq[A]` or written to disk. 

```scala
squares.fetch()                                                     // Seq[(Int, Int)]
write("hdfs://emails.csv", new CSVOutputFormat[(Int, Int)])(emails) // Unit
```

#### Primitive Computations: `fold`s

The core processing abstraction provided by `DataBag[A]` is a generic pattern for parallel collection processing called *structural recursion*. 

Assume for a moment that `DataBag[A]` instances can be constructed in one of three ways: As the `Empty` bag, a singleton bag `Sng(x)`, or the union of two existing bags `Union(xs, ys)`. *Structural recursion* works on bags by 

1. systematically deconstructing the input `DataBag[A]` instance, 
1. replacing the constructors with corresponding user-defined functions, and 
1. evaluating the resulting expression.

Formally, the above procedure can be specified as the following second-order function called `fold`.

```scala
def fold[B](e: B)(s: A => B, u: (B, B) => B): B = this match {
  case Empty         => e
  case Sng(x)        => s(x)
  case Union(xs, ys) => u(xs.fold(e)(s, u), ys.fold(e)(s, u))
}
```

In the above signature, `e` substitutes `Empty`, `s(x)` substitutes `Sng(x)`, and `u(xs, ys)` substitutes `Union(xs, ys)`.

Various collection processing primitives can be specified as a `fold`. 

```scala 
val min = xs.fold(Int.MaxValue)(identity, Math.min(_,_))
val max = xs.fold(Int.MinValue)(identity, Math.max(_,_))
val sum = xs.fold(0, x => 1, _ + _)
```

*Emma* offers [pre-defined aliases for common `fold` operators](emma-language/src/main/scala/eu/stratosphere/emma/macros/Folds.scala):

Fold Alias                     | Purpose
-------------------------------|-----------------------------
`min`, `max`, `sum`, `count` … | Aggregation
`exists`, `forall`, …          | Existential Qualifiers

#### Declarative Dataflows

Joins and cross products in *Emma* can be declared using `for`-comprehension syntax in a manner akin to *Select-From-Where* expressions known from SQL. 

```scala
for {
  email <- emails
  from  <- people
  to    <- people
  if email.from == from.email  
  if email.to   == to.email
} yield(email, from, to) // DataBag[(Email, Person, Person)]
```

In addition *Emma* offers the following bag combinators:

```scala
val csx = DataBag(Seq("Meijer", "Beckmann", "Wadler")) // computer scientists
val fbx = DataBag(Seq("Meijer", "Pelé", "Meijer"))     // footballers

// union (with bag semantics)
csx plus fbx 
// res: DataBag(Seq("Meijer", "Beckmann", "Pelé", "Wadler", "Meijer", "Meijer"))

// difference (with bag semantics)
fbx minus csx
// res: DataBag(Seq("Pelé", "Meijer"))

// duplicate elimination
fbx distinct 
// res: DataBag(Seq("Meijer", "Pelé"))
```

#### Nesting

In addition to the collection manipulation primitives presented above, *Emma* offers a `groupBy` combinator, which (conceptually) groups bag elements by key and introduces a level of nesting. 

To compute the average size of email messages by sender, for example, one can simply write the following expression. 

```scala 
for {
  (sender, mails) <- emails.groupBy(_.from) 
} yield {
  val sum = mails.map.(_.msg.length).sum()
  val cnt = mails.count()
  sum / cnt
}
```

#### DSL Compiler

The presented API is not abstract: The semantics of each operator are given directly by the corresponding [`DataBag[A]`-method definitions](emma-language/src/main/scala/eu/stratosphere/emma/api/DataBag.scala). This allows you to incrementally develop, test, and debug data analysis algorithms at small scale locally as a pure Scala programs.

For example, the following code-snippet that counts words runs out-of-the-box. 

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

Once the algorithm is ready, simply wrap it in the `emma.parallelize` macro which modifies the code and creates `Algorithm` object. The returned object can be executed on a co-processor engine like *Flink* or *Spark* (for scalable data-parallel execution) or the `Native` runtime (which will run the quoted code fragment unmodified). 

```scala
val algorithm = emma.parallelize {
  // word count code from above goes here!
}

// execute the algorithm on a parallel execution engine
algorithm.run(runtime.factory("flink"))
```

The `emma.parallelize` macro identifies all `DataBag[A]` terms in the quoted code fragment and re-writes them jointly in order to maximize the degree of parallelism. For example, the `groupBy` and the subsequent `fold`s from the [nesting](#nesting) example are executed using more efficient target-runtime primitives like `reduceByKey`. The holistic translation approach also allows us to transparently insert co-processor primitives like `cache`, `broadcast`, and `partitionBy` based on static analysis of the quoted code. 

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
