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

/**
 * Sums the elements in the bag. (non-grouped)
 * This can be used both as a combiner and a final reduce step, it just depends on its parallelism. (But note that it doesn't emit if the sum is 0.)
 */
public class SumCombinerDouble extends BagOperator<Double, Double> {

    private double sum = -1000000000;

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        sum = 0;
    }

    @Override
    public void pushInElement(Double e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        sum += e;
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (sum != 0) {
            out.collectElement(sum);
        }
        out.closeBag();
    }
}
