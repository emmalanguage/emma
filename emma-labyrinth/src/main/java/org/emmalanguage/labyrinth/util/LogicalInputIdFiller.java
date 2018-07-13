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

package org.emmalanguage.labyrinth.util;

import org.emmalanguage.labyrinth.ElementOrEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class LogicalInputIdFiller<T> implements MapFunction<ElementOrEvent<T>,ElementOrEvent<T>> {

    private final byte logicalInputId;

    public LogicalInputIdFiller(int logicalInputId) {
        this.logicalInputId = (byte)logicalInputId;
    }

    @Override
    public ElementOrEvent<T> map(ElementOrEvent<T> e) throws Exception {
        e.logicalInputId = logicalInputId;
        return e;
    }
}
