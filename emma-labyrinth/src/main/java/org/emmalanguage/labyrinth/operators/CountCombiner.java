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

public class CountCombiner<T> extends BagOperator<T, Integer> {

    private int count = -1000000000;

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        count = 0;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        count += 1;
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (count != 0) { // it's important that we send output only if we got input
            out.collectElement(count);
        }
        out.closeBag();
    }
}
