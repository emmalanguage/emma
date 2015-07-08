---
title: Quickstart
date: 2015-07-03 11:21:41
---

The [emma-quickstart]() module contains a maven archetype to quickly setup and run an Emma project.

To get started, download the emma-quickstart module (e.g. to `~/Downloads`), go to the directory you want to create 
your project in (e.g. `~/projects/emma`) and execute the `emma-quickstart.sh` script. This generates a maven project 
from the archetype that you can then import into your favorite IDE. If you want to use a distributed runtime 
(even if just locally) you should activate one of the available maven profiles (`flink` or `spark`). Otherwise it will 
still run as a Scala program without any further configurations.

The project includes a basic frame for an out-of-the-box [Emma Job](http://link-to-job) and a corresponding 
integration test in the `src/test` package. The `main/Job.scala` provides a framework to try out Emma and implement 
your first programs. You can test your programs on different runtimes by running the `JobTest.scala` in 
the `src/test` package.