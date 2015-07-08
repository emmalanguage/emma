---
title: "Emma Basics"
date: 2015-07-03 11:50:40
---

Import the **Emma** API into your algorithm namespace:

```scala
// required for all Emma programs
import eu.stratosphere.emma.api._
// required only for this tutorial
import Math.{pow, sqrt} 
```

**Emma** offers a core type called DataBag which is used to represent a parallel collection and computations on it (similar to Spark's RDD and Flink's DataSet). The simplest way to construct a DataBag is from a Scala sequence (i.e., a Seq object). The following line creates a DataBag containing the integers from 1 to 50:

```scala
val numbers = DataBag(1 to 50)
```

```
output: eu.stratosphere.emma.api.DataBag@44635af1
```

DataBag instances can be transformed into other DataBag by means of element-wise mapping using the map operator. For example, the following line computes the roots of all numbers:

```scala
val roots = numbers.map(n => (n, sqrt(n).toInt))
```

```
output: eu.stratosphere.emma.api.DataBag@6b4509e3
```

DataBags can be filetered using the withFilter operator. The following line will filter only pairs which represent exact roots:

```scala
val wholeRoots = numbers
                .withFilter(n => n == pow(sqrt(n).toInt, 2))
                .map(n => (n, sqrt(n).toInt))
```

```
output: eu.stratosphere.emma.api.DataBag@386e9a9f
```

To get the results of a parallel **Emma** DataBag converted back as a Scala sequence, use the fetch operator:

```scala
val res = wholeRoots.fetch()
```

```
output: Vector((1,1), (4,2), (9,3), (16,4), (25,5), (36,6), (49,7))
```

The DataBag type also ships with a generic fold as well as pre-defined aliases for specific folds, e.g. sum:

```scala
// sum up the numbers and the roots
(wholeRoots.map(_._1).sum(), wholeRoots.map(_._2).sum())
```

```
output: (140,28)
```