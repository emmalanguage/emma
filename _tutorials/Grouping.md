---
title: "Grouping"
date: 2015-07-03 13:10:30
---

Grouping is achieved trough the `groupBy` operator which receives a group key selector function of type `A => K` as an 
argument. In contrast to other related APIs, the result of the `groupBy` operator has the type 
`DataBag[Group[K,DataBag[A]]`, i.e. the elements are groups and the group values are again of type `DataBag`.

This allows for the definition of nested programs and facilitates non-trivial algebraic rewrites based on the `fold` 
restriction discussed above.

For example, the following code will compute several aggregates for the IMDB Top 100 list in parallel. Moreover, the 
computation of these aggregates will require only a single pass of the input data and will be pushed to the data 
in a parallel setup.

```scala
case class Aggregate(span: String, total: Long, 
    avg: Double, min: Double, max: Double) {}

val imdbStats = for (g <- imdbTop100.groupBy(_.year / 10)) yield {
  val total = g.values.count()
  val avgRating = g.values.map(_.rating).sum() / total
  val minRating = g.values.map(_.rating).min()
  val maxRating = g.values.map(_.rating).max()

  Aggregate(s"${g.key * 10} - ${g.key * 10 + 9}", 
    total, avgRating, minRating, maxRating)
}

println()
println("IMDB TOP 100 Aggregate Statistics:")
println("----------------------------------------")
println("FROM - TO   | TOTAL | AVG  | MIN  | MAX ")
println("----------------------------------------")
imdbStats.fetch().sortBy(_.span).reverse.foreach(agg => {
  print(f"${agg.span}   ")
  print(f"${agg.total}%-5d   ")
  print(f"${agg.avg}%2.2f   ")
  print(f"${agg.min}%2.2f   ")
  println(f"${agg.max}%2.2f") 
})
```

```
output:
IMDB TOP 100 Aggregate Statistics:
----------------------------------------
FROM - TO   | TOTAL | AVG  | MIN  | MAX 
----------------------------------------
2010 - 2019   6       8.47   8.40   8.70
2000 - 2009   17      8.50   8.30   8.90
1990 - 1999   23      8.56   8.30   9.20
1980 - 1989   11      8.42   8.30   8.80
1970 - 1979   11      8.57   8.30   9.20
1960 - 1969   8       8.50   8.30   8.90
1950 - 1959   12      8.46   8.30   8.90
1940 - 1949   8       8.41   8.30   8.60
1930 - 1939   3       8.47   8.40   8.50
1920 - 1929   1       8.30   8.30   8.30
```