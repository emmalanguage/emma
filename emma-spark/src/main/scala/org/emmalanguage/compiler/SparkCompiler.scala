/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler

import spark.SparkSpecializeSupport

trait SparkCompiler extends Compiler with SparkSpecializeSupport {

  override lazy val implicitTypes: Set[u.Type] = API.implicitTypes ++ SparkAPI.implicitTypes

  trait NtvAPI extends ModuleAPI {
    //@formatter:off
    val sym               = api.Sym[org.emmalanguage.api.spark.SparkNtv.type].asModule

    val select            = op("select")
    val project           = op("project")
    val equiJoin          = op("equiJoin")

    override lazy val ops = Set(select, project, equiJoin)
    //@formatter:on
  }

  trait SparkExpAPI extends ModuleAPI {
    //@formatter:off
    val Column      = api.Type[org.apache.spark.sql.Column]
    val sym         = api.Sym[org.emmalanguage.api.spark.SparkExp.type].asModule

    // projections
    val rootProj    = op("rootProj")
    val nestProj    = op("nestProj")
    val rootStruct  = op("rootStruct")
    val nestStruct  = op("nestStruct")
    // comparisons
    val eq          = op("eq", List(2, 1))
    val ne          = op("ne", List(2, 1))
    val gt          = op("gt")
    val lt          = op("lt")
    val geq         = op("geq")
    val leq         = op("leq")
    // boolean
    val not         = op("not")
    val or          = op("or")
    val and         = op("and")
    // arithmetic
    val plus        = op("plus")
    val minus       = op("minus")
    val multiply    = op("multiply")
    val divide      = op("divide")
    val mod         = op("mod")
    // string
    val startsWith  = op("startsWith")

    val projections = Set(rootProj, nestProj, rootStruct, nestStruct)
    val comparisons = Set(eq, ne, lt, gt, leq, geq)
    val boolean     = Set(not, and, or)
    val arithmetic  = Set(plus, minus, multiply, divide, mod)
    val string      = Set(startsWith)

    val ops         = projections ++ comparisons ++ boolean ++ arithmetic ++ string
    //@formatter:on
  }

  trait SparkAPILike extends BackendAPI {
    lazy val Encoder = api.Type[org.apache.spark.sql.Encoder[Any]].typeConstructor
    lazy val SparkSession = api.Type[org.apache.spark.sql.SparkSession]

    lazy val implicitTypes = Set(Encoder, SparkSession)

    lazy val MutableBag = new MutableBagAPI(api.Sym[org.emmalanguage.api.SparkMutableBag[Any, Any]].asClass)

    lazy val MutableBag$ = new MutableBag$API(api.Sym[org.emmalanguage.api.SparkMutableBag.type].asModule)

    lazy val Ops = new OpsAPI(api.Sym[org.emmalanguage.api.spark.SparkOps.type].asModule)

    lazy val Ntv = new NtvAPI {}

    lazy val Exp = new SparkExpAPI {}
  }

  object SparkAPI extends SparkAPILike {
    lazy val DataBag = new DataBagAPI(api.Sym[org.emmalanguage.api.SparkRDD[Any]].asClass)

    lazy val DataBag$ = new DataBag$API(api.Sym[org.emmalanguage.api.SparkRDD.type].asModule)
  }

  object SparkAPI2 extends SparkAPILike {
    lazy val DataBag = new DataBagAPI(api.Sym[org.emmalanguage.api.SparkDataset[Any]].asClass)

    lazy val DataBag$ = new DataBag$API(api.Sym[org.emmalanguage.api.SparkDataset.type].asModule)
  }

}
