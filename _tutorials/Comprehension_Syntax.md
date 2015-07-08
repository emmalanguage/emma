---
title: "Comprehension Syntax"
date: 2015-07-03 12:20:10
---

Let's revise the expression from the [Basics](Emma_Basics.html) section from a more formal perspective. We computed the set of elements 
![wholeRoots](/img/tutorials/comprehension_syntax/wholeRoots.png) which consists of all pairs 
![(n,r)](/img/tutorials/comprehension_syntax/nr.png) where ![n in range](/img/tutorials/comprehension_syntax/nInRange.png) 
and ![n](/img/tutorials/comprehension_syntax/n.png). Mathematicians often resort to a well-known notation called *set comprehension syntax* to concisely write sentence like 
the previous one as a formal expression. Slightly generalizing using double brackets to denote a bag instead of a set, 
we can write as a declarative specification of the functional code from [Basics](Emma_Basics.html).

The anatomy of a comprehension is simple. It consists of a *head expression* before the vertical bar, and a list of 
*qualifiers* after the bar. A qualifier can be either a *generator* (binding a variable), or a *filter*. In the example 
above, ![(n,r)](/img/tutorials/comprehension_syntax/nr.png) is the head, ![n in range](/img/tutorials/comprehension_syntax/nInRange.png) is the only generator, 
and ![n](/img/tutorials/comprehension_syntax/n.png) the only filter.

Moreover, a comprehension expression can also be *nested* within a parent comprehension, either as the head, the 
right-hand side of a generator, or within a filter (as an argument of a top-level `fold`).

Modern programming languages support some flavour of comprehension syntax (e.g., **for**-comprehensions in Scala, 
list comprehensions in Python). **Emma** makes use of that fact by allowing users to define **for**-comprehensions over `DataBag` values. The `wholeRoots` expression, for example, can be written in **Emma** as:

```scala
val wholeRoots = for (
  n <- DataBag(1 to 50); 
  if n == pow(sqrt(n).toInt, 2)) yield (n, sqrt(n).toInt)
wholeRoots.fetch()
```

```
output: Vector((1,1), (4,2), (9,3), (16,4), (25,5), (36,6), (49,7))
```