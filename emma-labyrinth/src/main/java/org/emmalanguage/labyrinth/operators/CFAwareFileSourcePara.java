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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This version can read a each file in parallel.
 * Instead of a basename and integer input elements, it takes an InputFormatProvider, which creates InputFormats
 * from filenames. The input elements are the filenames.
 * Para should be 1.
 * ! This should be used together with a CFAwareFileSourceParaReader, which can have a >1 para.
 */
public abstract class CFAwareFileSourcePara<OT, IS extends InputSplit>
        extends BagOperator<String, InputFormatWithInputSplit<OT, IS>> {

    private static final Logger LOG = LoggerFactory.getLogger(CFAwareFileSourcePara.class);

    @Override
    public void openOutBag() {
        super.openOutBag();
        assert host.subpartitionId == 0; // we need para 1
    }

    @Override
    public void pushInElement(String filename, int logicalInputId) {
        super.pushInElement(filename, logicalInputId);

        InputFormat<OT, IS> inputFormat = getInputFormatFromFilename(filename);
        try {
            assert host.outs.size() == 1;
            for(IS inputSplit: inputFormat.createInputSplits(host.outs.get(0).partitioner.targetPara)) {
                out.collectElement(new InputFormatWithInputSplit<>(inputFormat, inputSplit));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert inputId == 0;
        out.closeBag();
    }

    protected abstract InputFormat<OT, IS> getInputFormatFromFilename(String filename);
}
