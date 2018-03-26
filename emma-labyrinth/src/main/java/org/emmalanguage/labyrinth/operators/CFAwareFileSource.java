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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Can have >1 para. Set partitioning to RoundRobin.
 * However, it reads a single file non-parallel.
 */
public abstract class CFAwareFileSource<T> extends BagOperator<Integer, T> {

    private static final Logger LOG = LoggerFactory.getLogger(CFAwareFileSource.class);

    private final String baseName;

    public CFAwareFileSource(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        try {
            String path = baseName + e;

            LOG.info("Reading file " + path);

            FileSystem fs = FileSystem.get(new URI(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))), 1024*1024);

            String line;
            line = br.readLine();
            while (line != null) {
                out.collectElement(parseLine(line));
                line = br.readLine();
            }
        } catch (URISyntaxException | IOException e2) {
            throw new RuntimeException(e2);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert inputId == 0;
        out.closeBag();
    }

    abstract protected T parseLine(String line);
}
