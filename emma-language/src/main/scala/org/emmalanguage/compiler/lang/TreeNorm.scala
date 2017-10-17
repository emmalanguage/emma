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
package compiler.lang

import compiler.Common
import core.Core

import scala.collection.mutable


trait TreeNorm extends Common {
  self: Core =>

  import UniverseImplicits._

  /**
   *  Starting at the return value of a let-block, resorts val definitions within the tree in a depth-first manner.
   *
   *  The order of ValDefs will be a post-order traversal of the DAG that has ValDefs as its nodes, and there is a
   *  directed edge from u to v if the RHS of u's ValDef refers to v's ValDef.
   *  Note that the order of the recursive calls from a single node is determined by the order in which the collect
   *  call on the RHS returns the ValRefs. For this reason, this has to be a bottom-up transform, so that if there is a
   *  let-block in the RHS of a ValDef then it should have already been normalized.
   *
   *  In other words, TreeNorm will transform "equivalent" programs into identical trees, where "programA equivalent
   *  programB" means that programA can be transformed into programB by reordering ValDefs.
   *
   *  e.g.:
   *  {{{
   *    val $x4 = d.id
   *    val $x3 = c.id
   *    val $x2 = b.id
   *    val $x1 = a.id
   *    val $t1 = (a.id, b.id)
   *    val $t2 = (c.id, d.id)
   *    val $== = (t1 == t2)
   *  }}}
   *
   *  ==Resort Into==
   *  {{{
   *    val $x1 = a.id
   *    val $x2 = b.id
   *    val $t1 = (a.id, b.id)
   *    val $x3 = c.id
   *    val $x4 = d.id
   *    val $t2 = (c.id, d.id)
   *    val $== = (t1 == t2)
   *  }}}
   */
  def treeNorm(tree: u.Tree): u.Tree = {
    api.BottomUp.transform {
      case Core.Lang.Let(valDefs, defDefs, ret) => {
        assert(defDefs.isEmpty, "Not yet implemented for DefDefs")
        val reorderedDefs = sortVals(Core.Lang.Let(valDefs, Seq(), ret), valDefs.toList)
        Core.Lang.Let(reorderedDefs, Seq(), ret)
      }
    }._tree(tree)
  }

  def sortVals(tree: u.Tree, valDefs: List[u.ValDef]): Seq[u.ValDef] = {
    val defs: Map[u.TermSymbol, u.ValDef] = valDefs.map{ case vd @ Core.Lang.ValDef(vdSym, _) => (vdSym, vd) }.toMap

    val sb = Seq.newBuilder[u.ValDef]
    val seen = mutable.Set.empty[u.TermSymbol]

    // conditional depth first traversal
    def condDfs(sym: u.TermSymbol): Unit = {
      if (!seen(sym) && defs.contains(sym)) dft(defs(sym))
    }

    // depth first traversal
    def dft(vd: u.ValDef): Unit = {
      vd match {
        case Core.Lang.ValDef(vdSym, rhs) => {
          seen += vdSym
          rhs.collect{ case Core.Lang.ValRef(sym) => {
            condDfs(sym)
          }}
          sb += vd
        }
      }
    }

    tree match {
      case Core.Lang.Let(_, Seq(), ret) => ret.collect{
        case Core.Lang.ValRef(sym) => condDfs(sym)
      }
    }

    sb.result
  }
}
