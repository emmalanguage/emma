---
title: "Comprehending SQL"
date: 2015-07-03 12:20:30
---

A substantial subset of SQL can be seen as a flavor of the comprehension syntax discussed above:

```sql
SELECT <head>
FROM   <generators>
WHERE  <filters>
```

Database Management Systems (DBMSs) will first translate SQL expressions from the class above into a logical 
(abstract) algebraic expression of `filter`, `project`, `cross`, and `join` operators, and then decide upon the optimal 
physical execution strategy for that expression.

Due to [Generalization (I)]({% post_url 2015-06-19-origin %}), parallel dataflow systems accept programs in a form close to the one of the logical 
expression, i.e. trees over abstract second-order operators. Due to it's native support for **for**-comprehensions, 
**Emma** allows a level of abstraction similar to SQL directly in the embedded language (Scala).

For example, consider a scenario where we have the following base types:

```scala
case class IMDBEntry(
  title: String, 
  rating: Double, 
  rank: Int, 
  link: String, 
  year: Int) {}
case class FilmFestivalWinner(
  year: Int, 
  title: String, 
  director: String, 
  country: String) {}
```

```scala
val basePath = new java.io.File("data")
    .getAbsolutePath().replaceAll(" ", "\\\\ ");
val imdbTop100 = read(s"$basePath/cinema/imdb.csv", 
    new CSVInputFormat[IMDBEntry]);
val berlinaleWinners = read(s"$basePath/cinema/berlinalewinners.csv", 
    new CSVInputFormat[FilmFestivalWinner]);
val cannesWinners = read(s"$basePath/cinema/canneswinners.csv", 
    new CSVInputFormat[FilmFestivalWinner]);
```  

Finding up all Berlinale and Cannes winners in the IMDB Top 100 list can be achieved in **Emma** with the following expressions:

```scala
// berlinale winners in the IMDB Top 100 
val berlinaleInTop100 = for (
  imdb    <- imdbTop100;
  bwinner <- berlinaleWinners; 
  if imdb.title == bwinner.title
) yield imdb.title

println()
println("Berlinale Winners in the IMDB Top 100:")
println("--------------------------------------")
berlinaleInTop100.fetch().foreach(println)
```

```
output:
Berlinale Winners in the IMDB Top 100:
--------------------------------------
12 Angry Men
Spirited Away
```

```scala
// cannes winners in the IMDB Top 100 
val cannesInTop100 = for (
  imdb      <- imdbTop100;
  cwinner   <- cannesWinners; 
  if imdb.title == cwinner.title
) yield imdb.title

println()
println("Cannes Winners in the IMDB Top 100:")
println("-----------------------------------")
cannesInTop100.fetch().foreach(println)
```

```
output:
Cannes Winners in the IMDB Top 100:
-----------------------------------
Pulp Fiction
Apocalypse Now
The Pianist
Taxi Driver
```

This alleviates the need of binary operators like `cross` and `join` in the **Emma** API.