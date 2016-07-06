package eu.stratosphere
package emma.compiler

import emma.ast.MacroAST

import scala.reflect.macros.blackbox

/** A macro-based [[Compiler]]. */
class MacroCompiler(val c: blackbox.Context) extends Compiler with MacroAST
