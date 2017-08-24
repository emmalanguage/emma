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
package repl

import api._

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class ClassBasedScalaREPLSpec extends FlatSpec with Matchers {

  import ClassBasedScalaREPLSpec._

  "class-based Scala REPL" should "compile and evaluate properly" in {
    $line4.$read.INSTANCE.$iw.$iw.act should contain theSameElementsAs $line3.$read.INSTANCE.$iw.$iw.exp
  }
}

object ClassBasedScalaREPLSpec extends SparkAware {

  object $line3 {

    class $read extends Serializable {

      class $iw extends Serializable {

        class $iw extends Serializable {
          val exp = Seq(21)
        };
        val $iw = new $iw()
      };
      val $iw = new $iw()
    }

    object $read extends scala.AnyRef {
      val INSTANCE = new $read()
    }

  }

  object $line4 {

    class $read extends Serializable {

      class $iw extends Serializable {
        val $line3$read = $line3.$read.INSTANCE;

        import $line3$read.$iw.$iw.exp

        class $iw extends Serializable {

          val act = withDefaultSparkSession(implicit spark => emma.onSpark {
            DataBag(exp).collect()
          })
        }

        val $iw = new $iw()
      }

      val $iw = new $iw()
    }

    object $read extends scala.AnyRef {
      val INSTANCE = new $read()
    }

  }

}
