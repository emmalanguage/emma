/**
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
package org.emma.data.code;

import com.google.gson.JsonObject;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.*;
import java.util.Map;

/**
 * Interface of example loader. Loads examples for the gui, its source code and the configuration of the input parameter.
 */
public abstract class ExampleLoader {

    /**
     * Loads all classes that extend from the Algorithm class and returns the names.
     *
     * @return a map with the package name as key and the class name as value.
     */
    public abstract Map<String, String> getExampleNames();

    /**
     * Converts the package name to a path.
     *
     * @param name package name
     * @return a path to the scala file.
     */
    public abstract String getExamplePath(String name);

    public abstract Namespace getParameters(String className);

    /**
     * Loads a file and returns the source code.
     *
     * @param filePath file path of the scala file
     * @return the source code of the complete file
     */
    public String loadExampleSourceCode(String filePath) throws FileNotFoundException {
        StringBuilder sourceCode = new StringBuilder();

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("." + filePath);

        if (is == null)
            throw new FileNotFoundException(filePath + " not found.");

        char[] buffer = new char[1000];

        try (Reader in = new InputStreamReader(is)) {
            int readCount;
            while ((readCount = in.read(buffer, 0, buffer.length)) > 0) {
                sourceCode.append(buffer, 0, readCount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sourceCode.toString().trim();
    }

    public abstract JsonObject loadComprehensionBoxes(String className);
}
