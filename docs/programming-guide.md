---
layout: simple
title: Programming Guide
---

# {{ page.title }}

Emma programs require the following import.

```scala
import org.emmalanguage.api._
```

The examples below assume the following domain schema: 

```scala
case class Person(id: Long, email: String, name: String)
case class Email(id: Long, from: String, to: String, msg: String)
case class Movie(id: Long, year: Int, title: String)
```

## Primitive Type: DataBag

Parallel computation in Emma is represented by expressions over a core type `DataBag[A]`
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
emails.collect()                                            // Seq[Email]
movies.writeCSV("hdfs://movies.csv", CSV())               // Unit
squares.writeParquet("hdfs://squares.parquet", Parquet()) // Unit
```

## Primitive Computations: fold

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
def fold[B](zero: B)(init: A => B, plus: (B, B) => B): B = this match {
  case Empty         => zero
  case Singleton(x)  => init(x)
  case Union(xs, ys) => plus(xs.fold(e)(s, u), ys.fold(e)(s, u))
}
```

In the above signature, `zero` substitutes `Empty`,
`init(x)` substitutes `Singleton(x)`,
and `plus(xs, ys)`substitutes `Union(xs, ys)`.

Various collection processing primitives can be specified as a `fold`. 

```scala
val size = xs.fold(0L)(const(1L), _ + _)
val min  = xs.fold(None)(Some(_), lift(_ min _))
val sum  = xs.fold(0)(identity, _ + _)
```

Emma offers pre-defined aliases for common `fold` operators:

Fold Alias                          | Purpose
------------------------------------|-----------------------------
`reduce`, `reduceOption`            | General
`isEmpty`, `nonEmpty`, `size`       | Cardinality
`min`, `max`, `top`, `bottom`       | Order-based aggregation
`sum`, `product`                    | Numeric aggregation
`exists`, `forall`, `count`, `find` | Predicate testing
`sample`                            | Random sampling

## Declarative Dataflows

Joins and cross products in Emma can be declared using `for`-comprehension syntax
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

In addition Emma offers the following bag combinators:

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

## Nesting

In addition to the collection manipulation primitives presented above,
Emma offers a `groupBy` combinator, which (conceptually) groups bag elements by key
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

## Quotations

The presented API is not abstract. The semantics of each operator are given directly by the
default implementation in
[`ScalaSeq[A]`](emma-language/src/main/scala/org/emmalanguage/api/ScalaSeq.scala).
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

Once the algorithm is ready, simply quote it with one of the `emma.onSpark` or `emma.onFlink` macros
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
such as `cache` and `broadcast` based on static analysis of the quoted code.
