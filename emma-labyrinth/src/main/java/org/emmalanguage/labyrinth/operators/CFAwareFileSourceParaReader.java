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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;

public class CFAwareFileSourceParaReader<OT, IS extends InputSplit>
        extends BagOperator<InputFormatWithInputSplit<OT, IS>, OT> {

    private final TypeInformation<OT> outTypeInfo;
    private final TypeSerializer<OT> outSerializer;

    public CFAwareFileSourceParaReader(TypeInformation<OT> outTypeInfo) {
        this.outTypeInfo = outTypeInfo;
        outSerializer = outTypeInfo.createSerializer(new ExecutionConfig());
    }

    @Override
    public void pushInElement(InputFormatWithInputSplit<OT, IS> e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        try {

            InputFormat<OT, IS> format = e.inputFormat;
            IS inputSplit = e.inputSplit;

            if (format instanceof RichInputFormat) {
                ((RichInputFormat) format).openInputFormat();
            }

            format.open(inputSplit);

            while (!format.reachedEnd()) {
                OT nextElement = format.nextRecord(outSerializer.createInstance());
                if (nextElement != null) {
                    out.collectElement(nextElement);
                } else {
                    break;
                }
            }

            format.close();

            if (format instanceof RichInputFormat) {
                ((RichInputFormat) format).closeInputFormat();
            }

        } catch (IOException e1) {
            throw new RuntimeException();
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert inputId == 0;
        out.closeBag();
    }
}
