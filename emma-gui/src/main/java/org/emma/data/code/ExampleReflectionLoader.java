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
package org.emma.data.code;

import com.google.gson.JsonObject;
import eu.stratosphere.emma.examples.Algorithm;
import net.sourceforge.argparse4j.inf.Namespace;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Loads the examples with reflection out of the emma.examples package.
 */
public class ExampleReflectionLoader extends ExampleLoader {

    private Set<Class<? extends Algorithm>> exampleClasses = new HashSet<Class<? extends Algorithm>>();
    private String examplePackageName = "eu.stratosphere.emma.examples";

    @Override
    public Map<String, String> getExampleNames() {

        if (exampleClasses.size() == 0)
            loadExamples();

        HashMap<String, String> names = new HashMap<String, String>();

        for (Class<? extends Algorithm> clazz : exampleClasses) {
            String[] splits = clazz.getName().split("\\.");
            String name = splits[splits.length - 1];

            names.put(clazz.getName(), name);
        }

        return names;
    }

    @Override
    public String getExamplePath(String name) {
        return "/" + name.replaceAll("\\.", "/") + ".scala";
    }

    @Override
    public Namespace getParameters(String className) {
        return null;
    }

    private void loadExamples() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(false))
                .setUrls(ClasspathHelper.forClassLoader(new ClassLoader[0]))
                .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix(examplePackageName))));

        exampleClasses = reflections.getSubTypesOf(Algorithm.class);
    }

    @Override
    public JsonObject loadComprehensionBoxes(String className) {
        return null;
    }
}
