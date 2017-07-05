---
layout: simple
title: Programming Guide
---

# Emma Programming Guide

This guide is based on [the OpenFlights.org data](openflights.org/data.html) as described in the [Meet Emma](https://docs.google.com/presentation/d/1IM6VhGGg--dx5dEnCJtWkD0JXCl9Sw-wzTgr3Cj6uig/edit?usp=sharing) presentation.
For an interactive version of this document, checkout the the [emma-tutorial](https://github.com/emmalanguage/emma-tutorial) project and run the [Programming Guide](https://github.com/emmalanguage/emma-tutorial/blob/v{{ site.current_version }}/notebooks/programming-guide.ipynb) Jupyter notebook.

## Basic API

Emma programs require the following import.


```scala
import org.emmalanguage.api._
```

### DataBag Basics

Parallel computation in Emma is represented by expressions over a generic type `DataBag[A]`. 
The type represents a distributed collection of elements of type `A` that do not have particular order and may contain duplicates.

`DataBag` instances are constructed in two ways, either from a Scala `Seq`


```scala
val squaresSeq = 1 to 42 map { x => (x, x * x) }
val squaresBag = DataBag(squaresSeq)
```

or by reading from a supported source, e.g. `CSV` or `Parquet`.


```scala
val csv = CSV(delimiter = ',')
val airports = DataBag.readCSV[Airport](file("airports.dat").toString, csv)
val airlines = DataBag.readCSV[Airline](file("airlines.dat").toString, csv)
val routes = DataBag.readCSV[Route](file("routes.dat").toString, csv)
```

Conversely, a `DataBag` can be converted to a `Seq`


```scala
val squaresSeq = squaresBag.collect()
```

or written to a supported file system.


```scala
airports.writeCSV(file("airports.copy.dat").toString, csv)
airlines.writeCSV(file("airlines.copy.dat").toString, csv)
routes.writeCSV(file("routes.copy.dat").toString, csv)
```

### Declarative Dataflows

In contrast to other distributed collection types such as Spark's `RDD` and Flink's `DataSet`, Emma's `DataBag` type is a proper monad. 
This means that joins and cross products in Emma can be declared using `for`-comprehension syntax in a manner akin to *Select-From-Where* expressions known from SQL.


```scala
val flightsFromBerlin = for {
  ap <- airports 
  if ap.city == Some("Berlin")
  rt <- routes
  if rt.srcID == Some(ap.id)
  al <- airlines
  if rt.airlineID == Some(al.id)
} yield (rt.src, rt.dst, al.name)
```

In addition to comprehension syntax, the `DataBag` interface offers some combinators.

You can `sample` `N` elemens using [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling).


```scala
val samples = routes.map(_.src).sample(3)
```

You can combine two `DataBag[A]` instances by taking their (duplicate preserving) `union`, which corresponds to `UNION ALL` clause in SQL.


```scala
val srcs = routes.map(_.src) 
val dsts = routes.map(_.dst)
val locs = srcs union dsts
```

You can eliminate duplicates with `distinct`, corresponds to the `DISTINCT` clause in SQL.


```scala
val dupls = locs.collect().size
val dists = locs.distinct.collect().size
```

You can extend all elements in a `DataBag` with a unique index.


```scala
val iroutes = routes.map(_.src).zipWithIndex
```

### Structural Recursion (Folds)

In addition to the API illustrated above, the core processing primitive provided by `DataBag[A]` is a generic pattern for parallel collection processing called *structural recursion* on bags in *union representation*.

To decode the meaning behind these terms, assume that instances of `DataBag[A]` are constructed exclusively by the following constructors: 

1. `Empty` denotes the empty bag, 
2. `Singleton(x)` denotes a singleton bag with exactly one element `x`,
3. `Union(xs, ys)` denotes the union of two existing bags `xs` and `ys`.

The assumption implies that each bag `xs` can be represented as a binary tree of constructor applications. The inner nodes of the tree are applications of the `Union` constructor, while the leafs are applications of `Empty` or `Singleton`.

*Structural recursion* on bags in *union representation* works by

1. systematically deconstructing the input `DataBag[A]` instance, 
2. replacing the constructors with corresponding user-defined functions, and 
3. evaluating the resulting expression.

Formally, the above procedure can be specified second-order function called `fold` as follows.

```scala
def fold[B](zero: B)(init: A => B, plus: (B, B) => B): B = this match {
  case Empty         => zero
  case Singleton(x)  => init(x)
  case Union(xs, ys) => plus(xs.fold(e)(s, u), ys.fold(e)(s, u))
}
```

Note how

1. `Empty` constructors are substituted by `zero` applications, 
2. `Singleton(x)` constructors are substituted by `init(x)` applications, and 
3. `Union(xs, ys)` constructors are substituted by `plus(xs, ys)` applications.

A particular combination of `zero`, `init`, and `plus` function therefore defines a specific function. For example,


```scala
val dupls = locs.fold(0L)(_ => 1L, _ + _)
```

is another way to compute the number of elements of `dupls`. Note that this expression will be evaluated **in parallel**, while the version we used above


```scala
val dupls = locs.collect().size
```

first converts the `DataBag` **dupls** into a local `Seq[String]` and then counts the number of elements on the local machine.

A convenient way to bundle a specific combination of functions passed to a `fold` is through a dedicated trait.

```scala
// defined in `org.emmalanguage.api.alg`
trait Alg[-A, B] extends Serializable {
  val zero: B
  val init: A => B
  val plus: (B, B) => B
}
```

and overload `fold` as follows:

```scala
def fold[B](zero: B)(init: A => B, plus: (B, B) => B): B = this match {
  case Empty         => zero
  case Singleton(x)  => init(x)
  case Union(xs, ys) => plus(xs.fold(e)(s, u), ys.fold(e)(s, u))
}
```

With this extension, we can name commonly used triples as specific `Alg` instances.

```scala
object Size extends Alg[Any, Long] {
  val zero: Long = 0
  val init: Any => Long = const(1)
  val plus: (Long, Long) => Long = _ + _
}
```

and define corresponding aliases for the corresponding `fold(alg)` calls.

```scala
def size: Long = this.fold(Size)
```

The following methods `DataBag` are pre-defined in the `DataBag` trait and delegate to to a `fold` with a specific `Alg` instance.


```scala
// cardinality tests
locs.size      // counts the number of elements
locs.nonEmpty  // checks if empty
locs.isEmpty   // checks if not empty
```


```scala
// based on an implicit `Ordering`
locs.min       // minimum
locs.max       // maximum
locs.top(3)    // top-K
locs.bottom(3) // bottom-K
```


```scala
// arithmetic operations
routes.map(_.stops).sum
DataBag(1 to 5).product
```


```scala
// predicate testing
routes.count(_.stops < 3)
routes.forall(_.stops < 3)
routes.exists(r => r.src == "FRA" && r.dst == "SFO")
routes.find(r => r.src == "FRA" && r.dst == "SFO")
```


```scala
// reducers
DataBag(1 to 5).reduce(1)(_ * _)
DataBag(1 to 5).reduceOption(_ * _)
DataBag(Seq.empty[Int]).reduceOption(_ * _)
```

## Code Parallelisation

To parallelise Emma code, you need to do two things

1. Setup a parallel dataflow framework (Flink or Spark)
2. Enclose the code fragment you want to parallelize in an `emma.onSpark` or `emma.onFlink` quote.

### Parallel Dataflow Backend Setup

Depending on the backend you want to use, run one of the two options below.

#### Option 1: Spark


```scala
// `emma.onSpark` macro
import $ivy.`org.emmalanguage:emma-spark:{{ site.current_version }}`
// `provided` dependencies expected by `emma-spark`
import $ivy.`org.apache.spark::spark-sql:2.1.0`
```


```scala
// required spark imports
import org.apache.spark.sql.SparkSession
```


```scala
// implicit Spark environment
implicit val backend = SparkSession.builder()
    .appName("Emma Programming Guide")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", Paths.get(sys.props("java.io.tmpdir"), "spark-warehouse").toUri.toString)
    .getOrCreate()
```

#### Option 2: Flink


```scala
import $ivy.`org.emmalanguage:emma-flink:{{ site.current_version }}`
import $ivy.`org.apache.flink::flink-scala:1.2.1`
import $ivy.`org.apache.flink::flink-clients:1.2.1`
```


```scala
import org.apache.flink.api.scala.ExecutionEnvironment
implicit val backend = ExecutionEnvironment.getExecutionEnvironment
backend.getConfig.disableSysoutLogging()
```


```scala
// Include flink target classes in the classpath
val codegenDir = org.emmalanguage.compiler.RuntimeCompiler.default.instance.codeGenDir
interp.load.cp(ammonite.ops.Path(codegenDir))
```

### Quotations

Once you have an implicit `SparkSession` or `ExecutionEnvironment` instance in scope, you can wrap code that you want to run in parallel in an `emma.onSpark` or `emma.onFlink` quote. 

The wrapped is optimized holistically by the Emma compiler, and `DataBag` expressions are offloaded to the corresponding parallel exeuction engine.

For convenience, we alias of the quote used in the examples below to `emma.onSpark`. Change the alias to `emma.onFlink` and re-evaluate the following code snippets to parallelize them on Flink.


```scala
val quote = emma.onSpark // or emma.onFlink
```


```scala
// evaluates as map and filter
val berlinAirports = quote {
  for {
    a <- airports
    if a.latitude > 52.3
    if a.latitude < 52.6
    if a.longitude > 13.2
    if a.longitude < 13.7
  } yield Location(
    a.name,
    a.latitude,
    a.longitude)
}
```


```scala
// evaluates as cascasde of joins
val rs = quote {
  for {
    ap <- airports
    rt <- routes
    al <- airlines
    if rt.srcID == Some(ap.id)
    if rt.airlineID == Some(al.id)
  } yield (al.name, ap.country)
}
```


```scala
// evaluates using partial aggregates (reduce / reduceByKey)
val aggs = quote {
  for {
    Group(k, g) <- routes.groupBy(_.src)
  } yield {
    val x = g.count(_.airline == "AB")
    val y = g.count(_.airline == "LH")
    k -> (x, y)
  }
}
```

## Code Modularity

To build domain-specific libraries based on Emma, enclose your function definitions in a top-level object and annotate this object with the `@emma.lib` annotation.


```scala
@emma.lib
object hubs {
  def apply(M: Int) = {
    val rs = for {
      Group(k, g) <- ({
        routes.map(_.src)
      } union {
        routes.map(_.dst)
      }).groupBy(x => x)
      if g.size < M
    } yield k

    rs.collect().toSet
  }
}

@emma.lib
object reachable {
  def apply(N: Int)(hubs: Set[String]) = {
     val hubroutes = routes
       .withFilter(r => hubs(r.src) && hubs(r.dst))

     var paths = hubroutes
       .map(r => Path(r.src, r.dst))
     for (_ <- 0 until N) {
       val delta = for {
         r <- hubroutes; p <- paths if r.dst == p.src
       } yield Path(r.src, p.dst)
       paths = (paths union delta).distinct
     }

     paths
  }
}
```


```scala
// evaluates as Spark RDD reduceByKey
val rs = quote {
  reachable(2)(hubs(50))
}
```
