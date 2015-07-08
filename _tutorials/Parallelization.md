---
title: "Parallelization"
date: 2015-07-03 13:40:00
---

Advanced data analysis can be defined by combining the dataflow syntax explained above with control flow constructs. 
When the algorithm is ready, the user can wrap the code in special `parallelize` method implemented as Scala Macro. 
The `parallelize` macro modifies the code and wraps it in an `Algorithm` object. The returned object can be executed 
against the native runtime (to run the original code for debugging) or against a parallel execution engine like 
Flink or Spark (for scalable data-parallel execution).

```scala
// create a parallel version of your algorithm code
val algorithm = emma.parallelize {
  // your original algorithm code is placed here
  // the code consists of 
  //   (1) normal Scala code and 
  //   (2) Emma DataBag expressions
  // the Emma backend will generate dataflow code for 
  // the appropriate runtime passed to the synthesized Algorithm
}

// execute the algorithm on a parallel execution engine
algorithm.run(runtime.factory("flink", 6123))
```

**Emma** `DataBag` expressions occuring within the bracketed algorithm code serve as *coarse-grained parallelization 
contracts* for the `parallelize` macro. Code occurring in **Emma** comprehensions or part of the **Emma** `DataBag` API 
will be rewritten and transformed into Flink or Spark dataflows during parallel execution. Moreover, the **Emma** 
compiler will analyze all dataflows holistically and will reason how to execute them in an optimal way in the 
context of the surrounding algorithm.