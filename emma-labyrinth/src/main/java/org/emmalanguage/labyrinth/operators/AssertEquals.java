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

package org.emmalanguage.labyrinth.operators;

import org.emmalanguage.labyrinth.util.TestFailedException;
import org.emmalanguage.labyrinth.util.Unit;

import java.io.Serializable;

// !!! Don't forget to set the parallelism to 1 !!!
public class AssertEquals<IN> extends SingletonBagOperator<IN, Unit> implements Serializable {

    private final IN x;

    public AssertEquals(IN x) {
        this.x = x;
    }

    @Override
    public void pushInElement(IN e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        if (!e.equals(x)) {
            throw new TestFailedException("AssertEquals failed: got: " + e + ", expected " + x);
        }
    }
}
