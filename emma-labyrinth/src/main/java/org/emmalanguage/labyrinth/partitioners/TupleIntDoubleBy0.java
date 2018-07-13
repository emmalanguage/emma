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

package org.emmalanguage.labyrinth.partitioners;

import org.emmalanguage.labyrinth.util.TupleIntDouble;

/**
 * Partition a bag of Tuple2s by f0.
 */
public class TupleIntDoubleBy0 extends Partitioner<TupleIntDouble> {

    public TupleIntDoubleBy0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(TupleIntDouble elem, short subpartitionId) {
        return (short)(elem.f0 % targetPara);
    }
}
