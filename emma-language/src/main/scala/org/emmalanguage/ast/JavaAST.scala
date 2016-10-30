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

import scala.reflect.macros.Attachments
import scala.reflect.runtime
import scala.reflect.runtime.JavaUniverse
import scala.tools.reflect.ToolBox

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait JavaAST extends AST {

  require(runtime.universe.isInstanceOf[JavaUniverse],
    s"Unsupported universe ${runtime.universe}.\nThe runtime compiler supports only JVM.")

  override val universe = runtime.universe.asInstanceOf[JavaUniverse]
  val tb: ToolBox[universe.type]

  import universe._
  import internal._

  private val logger =
    Logger(LoggerFactory.getLogger(classOf[JavaAST]))

  override def abort(msg: String, pos: Position = NoPosition): Nothing =
    throw new RuntimeException(s"Error at $pos: $msg")

  override def warning(msg: String, pos: Position = NoPosition): Unit =
    logger.warn(s"Warning at $pos: $msg")

  override private[ast] def freshNameSuffix: Char = 'r'

  // ---------------------------
  // Parsing and type-checking
  // ---------------------------

  private[emmalanguage] def eval[T](code: Tree): T =
    compile(unTypeCheck(code))().asInstanceOf[T]

  private[emmalanguage] override def parse(code: String): Tree =
    tb.parse(code)

  private[emmalanguage] override def typeCheck(tree: Tree, typeMode: Boolean = false): Tree =
    try if (typeMode) tb.typecheck(tree, tb.TYPEmode) else tb.typecheck(tree)
    catch {
      case ex: scala.tools.reflect.ToolBoxError => throw scala.tools.reflect.ToolBoxError(
        s"Typecheck failed for tree:\n================\n${api.Tree.show(tree)}\n================\n", ex)
    }

  private[emmalanguage] override def inferImplicit(tpe: Type): Tree =
    tb.inferImplicitValue(tpe)

  private[emmalanguage] def compile(tree: Tree): () => Any =
    try {
      val res = tb.compile(tree)
      typeCheck(u.reify {}.tree) // This is a workaround for https://issues.scala-lang.org/browse/SI-9932
      res
    } catch {
      case ex: scala.tools.reflect.ToolBoxError => throw scala.tools.reflect.ToolBoxError(
        s"Compilation failed for tree:\n================\n${api.Tree.show(tree)}\n================\n", ex)
    }

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  override val get: Getter = new Getter {
    override lazy val enclosingOwner: Symbol = typeCheck(q"val x = ()").symbol.owner
    override def meta(sym: Symbol): Attachments = attachments(sym)
    override def meta(tree: Tree): Attachments = attachments(tree)
  }

  private[ast] override val set: Setter = new Setter {
    override def name(sym: Symbol, name: Name): Unit = sym.name = name
    override def original(tpt: TypeTree, original: Tree): Unit = tpt.setOriginal(original)
    override def owner(sym: Symbol, owner: Symbol): Unit = sym.owner = owner
    override def pos(sym: Symbol, pos: Position): Unit = sym.pos = pos
    override def pos(tree: Tree, pos: Position): Unit = tree.pos = pos
    override def flags(sym: Symbol, flags: FlagSet): Unit = {
      sym.resetFlags()
      setFlag(sym, flags)
    }
  }
}
