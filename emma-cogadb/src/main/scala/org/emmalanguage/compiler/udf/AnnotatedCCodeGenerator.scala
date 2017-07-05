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
package compiler.udf


import scala.reflect.runtime.universe._

trait AnnotatedCCodeGenerator extends TypeHelper {

  private val reservedKeywords = Seq(
    "auto", "long", "switch", "break", "enum", "register", "typedef", "extern",
    "union", "char", "float", "short", "unsigned", "const", "signed", "void",
    "continue", "goto", "sizeof", "volatile",
    "default", "static", "int", "struct", "_Packed", "double")

  private val supportedUnaryMethods = Map(
    TermName("toDouble") -> "(double)",
    TermName("toFloat") -> "(float)",
    TermName("toInt") -> "(int32_t)",
    TermName("toLong") -> "(int64_t)"
  )

  private val supportedBinaryMethods = Map(
    TermName("$plus") -> '+,
    TermName("$minus") -> '-,
    TermName("$times") -> '*,
    TermName("$div") -> '/,
    TermName("$percent") -> '%,
    //    TermName("$up") -> '^,
    TermName("$eq$eq") -> '==,
    TermName("$less") -> '<,
    TermName("$less$eq") -> '<=,
    TermName("$greater") -> '>,
    TermName("$greater$eq") -> '>=
  )

  private val supportedLibraries = Seq(
    "scala.math.`package`", "scala.math"
  )

  protected def newUDFOutput(tpe: Type, infix: String = ""): TypeName

  def isSupportedUnaryMethod(name: Name): Boolean =
    if (name.isTermName) supportedUnaryMethods contains name.toTermName else false

  def isSupportedBinaryMethod(name: Name): Boolean =
    if (name.isTermName) supportedBinaryMethods contains name.toTermName else false


  def isSupportedLibrary(qualifier: Tree): Boolean = (supportedLibraries contains qualifier.toString) ||
    supportedLibraries.exists(qualifier.tpe.widen.toString.contains(_))

  def isInstantiation(name: Name): Boolean = name == TermName("apply")

  def Const(const: Constant): String =
    if (const.tpe == typeOf[String] || const.tpe == typeOf[java.lang.String]) "\"" + const.value + "\""
    else if (const.tpe == typeOf[Char]) "\'" + const.value + "\'"
    else s"${const.value}"

  def generateLocalVar(name: Name): String = s"$name"

  def generateLineStmt(stmt: String): String = s"$stmt;"

  def generateVarDef(tpe: Name, v: Name): String = s"$tpe $v"

  def generateColAccess(tblName: String, tblCol: String, toUpperCase: Boolean = true): String =
    if (toUpperCase) s"#$tblName.${tblCol.toUpperCase}#" else s"#$tblName.$tblCol#"

  def generateOutputExpr(col: TypeName): String

  def generateNoOp: String = ""

  def generateUnaryOp(op: Name, arg: String): String = supportedUnaryMethods get op.toTermName match {
    case Some(o) => s"$o($arg)"
    case None => s"$op($arg)"
  }

  def generateBinaryOp(op: Name, arg1: String, arg2: String): String = supportedBinaryMethods get op.toTermName match {
    case Some(o) => s"($arg1${o.name}$arg2)"
    case None => s"$op($arg1,$arg2)"
  }

  def generateAssignmentStmt(lhs: String, rhs: String): Seq[String] = Seq(generateLineStmt(s"$lhs=$rhs"))

  def generateIfThenElseStmt(cond: String, thenp: Seq[String], elsep: Seq[String]): Seq[String] =
    Seq(s"if($cond){") ++ thenp ++ Seq("}else{") ++ elsep ++ Seq("}")


  def generateAnnotatedCCode(symbolTable: Map[String, String], ast: Tree, isPotentialOut: Boolean = false):
  Seq[String] = {
    ast match {
      case Function(_, body) =>
        generateAnnotatedCCode(symbolTable, body, isPotentialOut)

      //      case DefDef(_, _, _, _, _, rhs) =>
      //        transformToMapUdfCode(rhs, isPotentialOut)

      //TODO: check if this case is really necessary
      case Block(Nil, expr) =>
        generateAnnotatedCCode(symbolTable, expr, isPotentialOut)

      case Block(xs, expr) =>
        xs.flatMap(generateAnnotatedCCode(symbolTable, _)) ++ generateAnnotatedCCode(symbolTable, expr, isPotentialOut)

      case Apply(typeApply: TypeApply, args: List[Tree]) =>
        transformTypeApply(symbolTable, typeApply, args, isPotentialOut)

      case app@Apply(sel: Select, args: List[Tree]) =>
        transformSelectApply(symbolTable, app, sel, args, isPotentialOut)

      case sel: Select =>
        transformSelect(symbolTable, sel, isPotentialOut)

      case Assign(lhs: Ident, rhs) =>
        generateAssignmentStmt(generateLocalVar(lhs.name), generateAnnotatedCCode(symbolTable, rhs).mkString)

      case ValDef(_, name, tpt: TypeTree, rhs) =>
        transformValDef(symbolTable, name, tpt, rhs)

      case ifAst: If =>
        transformIfThenElse(symbolTable, ifAst, isPotentialOut)

      case ide: Ident =>
        transformIdent(symbolTable, ide, isPotentialOut)

      case lit: Literal =>
        transformLiteral(lit, isPotentialOut)

      case _ => throw new IllegalArgumentException(s"Code generation for tree type not supported: ${showRaw(ast)}")
    }
  }

  private def transformValDef(symbolTable: Map[String, String], name: TermName, tpt: TypeTree, rhs: Tree):
  Seq[String] = {
    //TODO: create and apply regex for valid C identifiers
    if (reservedKeywords.contains(s"$name")) {
      throw new IllegalArgumentException(s"Please choose a new name for local variable $name.")
    }
    //    localVars += (name -> (tpt.tpe, name))
    generateAssignmentStmt(generateVarDef(tpt.tpe.toCPrimitive, name),
                           generateAnnotatedCCode(symbolTable, rhs).mkString)
  }

  protected def freshVarName: TermName

  private def transformIfThenElse(symbolTable: Map[String, String], ifAst: If, isPotentialOut: Boolean): Seq[String] = {
    if (isPotentialOut) {
      ifAst match {
        case If(cond, thenp: Block, elsep: Block) => {
          //both branches are block stmts
          val freshLocalVar = freshVarName
          val transformedCond = generateAnnotatedCCode(symbolTable, cond).mkString
          val transformedThenp = thenp.stats.flatMap(generateAnnotatedCCode(symbolTable, _)) ++
            generateAssignmentStmt(s"$freshLocalVar", generateAnnotatedCCode(symbolTable, thenp.expr).mkString)
          val transformedElsep = elsep.stats.flatMap(generateAnnotatedCCode(symbolTable, _)) ++
            generateAssignmentStmt(s"$freshLocalVar", generateAnnotatedCCode(symbolTable, elsep.expr).mkString)

          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
            generateAssignmentStmt(generateOutputExpr(newUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }

        case If(cond, thenp: Block, elsep) => {
          //only thenp is block stmt
          val freshLocalVar = freshVarName
          val transformedCond = generateAnnotatedCCode(symbolTable, cond).mkString
          val transformedThenp = thenp.stats.flatMap(generateAnnotatedCCode(symbolTable, _)) ++
            generateAssignmentStmt(s"$freshLocalVar", generateAnnotatedCCode(symbolTable, thenp.expr).mkString)
          val transformedElsep = generateAssignmentStmt(s"$freshLocalVar",
                                                        generateAnnotatedCCode(symbolTable, elsep).mkString)

          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
            generateAssignmentStmt(generateOutputExpr(newUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }

        case If(cond, thenp, elsep: Block) => {
          //only elsep is block stmt
          val freshLocalVar = freshVarName
          val transformedCond = generateAnnotatedCCode(symbolTable, cond).mkString
          val transformedThenp = generateAssignmentStmt(s"$freshLocalVar",
                                                        generateAnnotatedCCode(symbolTable, thenp).mkString)
          val transformedElsep = elsep.stats.flatMap(generateAnnotatedCCode(symbolTable, _)) ++
            generateAssignmentStmt(s"$freshLocalVar", generateAnnotatedCCode(symbolTable, elsep.expr).mkString)

          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
            generateAssignmentStmt(generateOutputExpr(newUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }

        case If(cond, thenp, elsep) => {
          //single then and else stmt
          if (ifAst.tpe.isScalaBasicType) {
            val freshLocalVar = freshVarName
            val valDef = generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))
            val transformedCond = generateAnnotatedCCode(symbolTable, cond).mkString
            val transformedThenp = generateAssignmentStmt(s"$freshLocalVar",
                                                          generateAnnotatedCCode(symbolTable, thenp).mkString)
            val transformedElsep = generateAssignmentStmt(s"$freshLocalVar",
                                                          generateAnnotatedCCode(symbolTable, elsep).mkString)
            val finalUDFStmt = generateAssignmentStmt(generateOutputExpr(newUDFOutput(ifAst.tpe)), s"$freshLocalVar")
            Seq(valDef) ++
              generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
              finalUDFStmt
          } else {
            //complex type
            //currently this case should only occur for filter UDF rewrites to flatMap UDF
            val transformedCond = generateAnnotatedCCode(symbolTable, cond).mkString
            //flatten thenp stmt
            val transformedThenp = generateAnnotatedCCode(symbolTable, thenp, true)
            //            val transformedElsep = Seq(generateLineStmt("NONE"))
            val transformedElsep = Seq()
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep)
          }
        }
      }
    } else {
      generateIfThenElseStmt(generateAnnotatedCCode(symbolTable, ifAst.cond).mkString,
        generateAnnotatedCCode(symbolTable, ifAst.thenp),
        generateAnnotatedCCode(symbolTable, ifAst.elsep))
    }
  }

  private def transformLiteral(lit: Literal, isFinalStmt: Boolean, infix: String = ""): Seq[String] = {
    if (isFinalStmt) {
      generateAssignmentStmt(generateOutputExpr(newUDFOutput(lit.tpe, infix)), Const(lit.value))
    } else {
      Seq(Const(lit.value))
    }
  }

  private def transformIdent(symbolTable: Map[String, String], ide: Ident, isFinalStmt: Boolean): Seq[String] = {
    if (isFinalStmt) {
      if (ide.tpe.isScalaBasicType) {
        //if input parameter, otherwise local variable
        if (symbolTable isDefinedAt ide.name.toString) {
          generateAssignmentStmt(generateOutputExpr(newUDFOutput(ide.tpe)),
                                 generateColAccess(symbolTable(ide.name.toString), basicTypeColumnIdentifier, false))
        } else {
          generateAssignmentStmt(generateOutputExpr(newUDFOutput(ide.tpe)), ide.name.toString)
        }
      } else {
        //flatten complex input param (UDT)
        transformAndFlattenComplexInputOutput(symbolTable(ide.name.toString), ide.tpe)
      }
    } else {
      if (symbolTable isDefinedAt ide.name.toString) {
        Seq(generateColAccess(symbolTable(ide.name.toString), basicTypeColumnIdentifier, false))
      } else {
        Seq(generateLocalVar(ide.name))
      }
    }
  }

  protected def basicTypeColumnIdentifier: String

  private def mapFieldNameToType(members: List[Symbol], types: List[Type]): List[(String, Type)] = {
    if (members.isEmpty) List()
    else (members.head.name.toString.trim, types.head) +:
      mapFieldNameToType(members.tail, types.tail)
  }

  private def transformAndFlattenComplexInputOutput(tblName: String, tpe: Type, infix: String = ""): Seq[String] = {
    val valueMembers = tpe.members.filter(!_.isMethod).toList.reverse
    var fieldNamesAndTypes = valueMembers.map(v => (v.name.toString.trim, v.typeSignature))
    if (tpe.isTuple) {
      val typeArgs = tpe.typeArgs
      fieldNamesAndTypes = mapFieldNameToType(valueMembers, typeArgs)
    }

    fieldNamesAndTypes.flatMap { field => {
      val currFieldName = field._1
      val currTpe = field._2
      if (currTpe.isScalaBasicType) {
        generateAssignmentStmt(
          generateOutputExpr(newUDFOutput(currTpe, s"$infix${currFieldName}_")),
          generateColAccess(tblName, s"$infix${currFieldName}"))
      } else {
        transformAndFlattenComplexInputOutput(tblName, currTpe, s"$infix${currFieldName}_")
      }
    }
    }
  }

  private def transformTypeApply(symbolTable: Map[String, String], typeApply: TypeApply, args: List[Tree],
    isFinalSmt: Boolean): Seq[String] = {
    //initialization of a Tuple type
    if (isFinalSmt) {
      transformComplexOutputInitialization(symbolTable, typeApply, true, args)
    } else {
      throw new IllegalArgumentException(s"Instantiation of ${typeApply.toString()} not allowed at this place.")
    }
  }

  //generate code for complex output initialization and flatten if necessary
  private def transformComplexOutputInitialization(symbolTable: Map[String, String],
    app: Tree, isComplex: Boolean, args: List[Tree],
    infix: String = ""): Seq[String] = {
    if (isComplex) {
      var mapped = mapFieldNameToArg(extractValueMembers(app.tpe), args)
      mapped.flatMap(
        tuple =>
          tuple._2 match {
            case lit: Literal => transformLiteral(lit, true, s"$infix${tuple._1}_")
            case appp@Apply(sell: Select, argss: List[Tree]) =>
              transformComplexOutputInitialization(symbolTable, appp, isInstantiation(sell.name), argss,
                s"$infix${tuple._1}_")
            case Apply(typeApply: TypeApply, args: List[Tree]) =>
              transformComplexOutputInitialization(symbolTable, typeApply, true, args, s"$infix${tuple._1}_")
            case _ =>
              generateAssignmentStmt(generateOutputExpr(newUDFOutput(tuple._2.tpe, s"$infix${tuple._1}_")),
                generateAnnotatedCCode(symbolTable, tuple._2).mkString)
                       })
    } else {
      generateAssignmentStmt(
        generateOutputExpr(newUDFOutput(app.tpe, infix)),
        generateAnnotatedCCode(symbolTable, app).mkString)
    }
  }

  private def extractValueMembers(tpe: Type): List[Symbol] =
    if (!tpe.members.isEmpty) tpe.members.filter(!_.isMethod).toList.reverse
    else tpe.paramLists.head

  private def mapFieldNameToArg(members: List[Symbol], args: List[Tree]): Seq[(String, Tree)] = {
    if (members.isEmpty) Seq()
    else (members.head.name.toString.trim, args.head) +: mapFieldNameToArg(members.tail, args.tail)
  }

  private def transformSelectApply(symbolTable: Map[String, String], app: Apply, sel: Select, args: List[Tree],
    isFinalSmt: Boolean): Seq[String] = {
    if (isFinalSmt) {
      //initialization of a complex type
      transformComplexOutputInitialization(symbolTable, app, isInstantiation(sel.name), args)
    } else {
      if (isSupportedBinaryMethod(sel.name)) {
        Seq(generateBinaryOp(sel.name.toTermName, generateAnnotatedCCode(symbolTable, sel.qualifier).mkString,
                             generateAnnotatedCCode(symbolTable, args.head).mkString))
      } else if (isSupportedLibrary(sel.qualifier)) {
        args.size match {
          case 1 => Seq(generateUnaryOp(sel.name.toTermName, generateAnnotatedCCode(symbolTable, args(0)).mkString))
          case 2 => Seq(generateBinaryOp(sel.name.toTermName, generateAnnotatedCCode(symbolTable, args(0)).mkString,
                                         generateAnnotatedCCode(symbolTable, args(1)).mkString))
          case _ => throw new IllegalArgumentException(s"Select ${sel.name.toString} with ${args.size} arguments " +
                                                         "not supported.")
        }
      } else {
        throw new IllegalArgumentException(s"Apply ${sel.name.toString} not supported.")
      }
    }
  }

  private def transformSelect(symbolTable: Map[String, String], sel: Select, isFinalStmt: Boolean): Seq[String] = {
    val split = sel.toString.split("\\.")
    if (sel.symbol.toString.startsWith("method") && isSupportedUnaryMethod(sel.name)) {
      val op = generateUnaryOp(sel.name.toTermName, generateAnnotatedCCode(symbolTable, sel.qualifier).mkString)
      if (isFinalStmt) {
        generateAssignmentStmt(generateOutputExpr(newUDFOutput(sel.tpe)), op)
      }
      else {
        Seq(op)
      }
    } else if (symbolTable isDefinedAt (split.head)) {
      val tblName = symbolTable(split.head)
      val colName = split.tail.mkString("_")
      if (sel.tpe.isScalaBasicType) {
        val colAccess = generateColAccess(tblName, colName)
        if (isFinalStmt) {
          generateAssignmentStmt(generateOutputExpr(newUDFOutput(sel.tpe)), colAccess)
        }
        else {
          Seq(colAccess)
        }
      } else {
        if (isFinalStmt) {
          transformAndFlattenComplexInputOutput(tblName, sel.tpe, colName)
        }
        else {
          throw new IllegalArgumentException(s"Value access for ${sel.name} not allowed at this place.")
        }
      }
    } else if (sel.symbol.toString.startsWith("method") && !isSupportedUnaryMethod(sel.name)) {
      throw new IllegalArgumentException(s"Method ${sel.name} not supported.")
    } else {
      throw new IllegalArgumentException(s"Missing table mapping for parameter ${split.head}.")
    }
  }

}
