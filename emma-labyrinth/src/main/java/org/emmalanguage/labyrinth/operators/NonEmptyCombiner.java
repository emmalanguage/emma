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
 * Don't forget to either set the parallelism to 1, or put an Or with para 1 after this.
 *
 * The difference to NonEmpty is that we don't send output when we haven't got input element.
 */
public class NonEmptyCombiner<T> extends BagOperator<T, Boolean> {

    private static final int closedNum = -1000;

    private int num = closedNum;
    private boolean sent = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        assert num == closedNum;
        num = 0;
        sent = false;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        num++;
        if (!sent) {
            out.collectElement(true);
            sent = true;
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
//        if (num == 0) {
//            out.collectElement(false);
//        }
        num = closedNum;
        out.closeBag();
    }
}
