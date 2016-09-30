package org.emmalanguage
package compiler

import ast.MacroAST

import scala.reflect.macros.blackbox

/** A macro-based [[Compiler]]. */
trait MacroCompiler extends Compiler with MacroAST
