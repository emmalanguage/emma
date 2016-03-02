package eu.stratosphere.emma
package compiler

import scala.reflect.macros.blackbox

/** A macro-based [[Compiler]]. */
class MacroCompiler(val c: blackbox.Context) extends Compiler with MacroUtil
