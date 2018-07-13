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

// import org.apache.flink.api.java.tuple.Tuple2;
import scala.Tuple2;
import org.emmalanguage.labyrinth.util.SerializedBuffer;
import scala.util.Either;

public class Cross<A, B> extends BagOperator<Either<A,B>, Tuple2<A,B>> implements ReusingBagOperator {

	private SerializedBuffer<Either<A,B>> lhsBuffered; // build
    private SerializedBuffer<Either<A,B>> rhsBuffered;
    private boolean lhsDone;
    private boolean rhsDone;

    private boolean reuse = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        rhsBuffered = new SerializedBuffer<>(inSer);
        lhsDone = false;
        rhsDone = false;
        reuse = false;
    }

    @Override
    public void signalReuse() {
        reuse = true;
    }

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);

        if (logicalInputId == 0) {
            // lhs
            if (!reuse) {
                lhsBuffered = new SerializedBuffer<>(inSer);
            }
        }
    }

    @Override
    public void pushInElement(Either<A,B> e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // lhs side
            assert !lhsDone;
            lhsBuffered.add(e);
        } else { // rhs
            if (!lhsDone) {
                rhsBuffered.add(e);
            } else {
                generateTuples(e);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (inputId == 0) { // lhs
            assert !lhsDone;
            lhsDone = true;
            for (Either<A,B> b: rhsBuffered) {
                generateTuples(b);
            }
            if (rhsDone) {
                out.closeBag();
            }
        } else { // rhs
            assert inputId == 1;
            assert !rhsDone;
            rhsDone = true;
            if (lhsDone) {
                out.closeBag();
            }
        }
    }

    private void generateTuples(Either<A,B> b) {
        for (Either<A,B> a: lhsBuffered) {
            out.collectElement(Tuple2.apply(a.left().get(), b.right().get()));
        }
    }
}
