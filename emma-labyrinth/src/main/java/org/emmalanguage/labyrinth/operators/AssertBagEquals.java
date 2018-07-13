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

import org.emmalanguage.labyrinth.util.Nothing;
import org.emmalanguage.labyrinth.util.TestFailedException;

import java.util.HashMap;

// !!! Don't forget to set the parallelism to 1 !!!
public class AssertBagEquals<IN> extends BagOperator<IN, Nothing> {

    private final HashMap<IN, Integer> expected;

    private HashMap<IN, Integer> counts;

    @SafeVarargs
    public AssertBagEquals(IN... ref) {
        expected = new HashMap<>();
        for (IN x: ref) {
            Integer g = expected.get(x);
            if (g == null) {
                expected.put(x, 1);
            } else {
                expected.replace(x, g + 1);
            }
        }
    }

    @Override
    public void openOutBag() {
        super.openOutBag();
        counts = new HashMap<>();
    }

    @Override
    public void pushInElement(IN e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        Integer g = counts.get(e);
        if (g == null) {
            counts.put(e, 1);
        } else {
            counts.replace(e, g + 1);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        for (HashMap.Entry<IN, Integer> a: counts.entrySet()) {
            Integer expCount0 = expected.get(a.getKey());
            if (expCount0 == null) {
                throw new TestFailedException("AssertBagEquals failed: got unexpected element " + a.getKey() + " " + a.getValue() + " times");
            }
            int expCount = expCount0;
            int actCount = a.getValue();
            if (expCount != actCount) {
                throw new TestFailedException("AssertBagEquals failed: element: " + a.getKey() + ", expected count: " + expCount + ", actual count: " + actCount);
            }
        }

        for (HashMap.Entry<IN, Integer> e: expected.entrySet()) {
            if (counts.get(e.getKey()) == null) {
                throw new TestFailedException("AssertBagEquals failed: expected element " + e.getKey() + " did not appear");
            }
        }

        out.closeBag();
    }
}
