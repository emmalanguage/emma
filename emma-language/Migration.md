# Compiler API migration guide

This a correspondance table that can be used when migrating code written against the (old) `eu.stratosphere.emma.compiler` API and the (new) `eu.stratosphere.emma.ast.AST.api` API. 


| System           | Version        | Improvement       |
| ---------------- | -------------- | ----------------- |
| `Term sym tree`  | `Sym of tree`  | `TermSym of tree` |
