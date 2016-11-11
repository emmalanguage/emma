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
package ast

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.reflect.runtime
import scala.reflect.runtime.JavaUniverse
import scala.tools.reflect.ToolBox
import scala.tools.reflect.ToolBoxError

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait JavaAST extends AST {

  require(runtime.universe.isInstanceOf[JavaUniverse],
    s"Unsupported universe ${runtime.universe}.\nThe runtime compiler supports only JVM.")

  val universe = runtime.universe.asInstanceOf[JavaUniverse]
  val tb: ToolBox[universe.type]

  import universe._

  private val logger =
    Logger(LoggerFactory.getLogger(classOf[JavaAST]))

  def freshNameSuffix = 'r'

  // ---------------------------
  // Parsing and type-checking
  // ---------------------------

  def abort(msg: String, pos: Position = NoPosition) =
    throw ToolBoxError(s"Error at $pos:\n$msg")

  def warning(msg: String, pos: Position = NoPosition) =
    logger.warn(s"Warning at $pos\n$msg")

  def parse(code: String) = try tb.parse(code) catch {
    case err: ToolBoxError => throw ToolBoxError(s"""
      |Parsing failed for tree:
      |================
      |$code
      |================
      |""".stripMargin.trim, err)
  }

  def typeCheck(tree: Tree, typeMode: Boolean = false) =
    try if (typeMode) tb.typecheck(tree, tb.TYPEmode) else tb.typecheck(tree)
    catch { case err: ToolBoxError => throw ToolBoxError(s"""
      |Type-check failed for tree:
      |================
      |${showCode(tree)}
      |================
      |""".stripMargin.trim, err)
    }

  def compile(tree: Tree): () => Any = try {
    val binary = tb.compile(tree)
    // This is a workaround for https://issues.scala-lang.org/browse/SI-9932
    typeCheck(reify(()).tree)
    binary
  } catch { case err: ToolBoxError => throw ToolBoxError(s"""
    |Compilation failed for tree:
    |================
    |${showCode(tree)}
    |================
    |""".stripMargin.trim, err)
  }

  def eval[T](code: Tree) =
    compile(unTypeCheck(code))().asInstanceOf[T]

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  lazy val enclosingOwner =
    typeCheck(q"val x = ()").symbol.owner

  def inferImplicit(tpe: Type) = for {
    value <- Option(tb.inferImplicitValue(tpe))
    if value.nonEmpty
  } yield value.setType(value.tpe.finalResultType)

  def setOriginal(tpt: TypeTree, original: Tree) =
    tpt.setOriginal(original)

  def setPos[T <: Tree](tree: T, pos: Position) =
    internal.setPos(tree, pos)
}
