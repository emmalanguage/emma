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
import org.emmalanguage.labyrinth.util.SerializedBuffer;
import scala.Unit;

import scala.util.Either;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class FileSinkString extends BagOperator<String, Unit> implements DontThrowAwayInputBufs {

    private String currentFileName;

    private boolean[] closed = new boolean[2];

    private SerializedBuffer<String> buffer;

    @Override
    public void openOutBag() {
        super.openOutBag();

        assert host.subpartitionId == 0; // we need para 1

        buffer = new SerializedBuffer<>(inSer);

        closed[0] = false;
        closed[1] = false;

        currentFileName = null;
    }

    @Override
    public void pushInElement(String e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        if (logicalInputId == 0) {
            currentFileName = e;
        } else {
            assert logicalInputId == 1;
            buffer.add(e);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        closed[inputId] = true;

        if (closed[0] && closed[1]) {
            try {

                FileSystem fs = FileSystem.get(new URI(currentFileName));
                //BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(currentFileName))));

                //PrintWriter csvWriter = new PrintWriter(currentFileName, "UTF-8");
                PrintWriter writer = new PrintWriter(fs.create(new Path(currentFileName), FileSystem.WriteMode.OVERWRITE));
                for (String e : buffer) {
                    writer.println(e);
                }
                buffer = null;
                writer.close();
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException(e);
            }

            out.closeBag();
        }
    }
}
