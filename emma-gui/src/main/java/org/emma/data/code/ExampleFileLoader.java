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

import com.google.gson.*;
import net.sourceforge.argparse4j.inf.Namespace;
import org.emma.config.ConfigReader;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads example configuration from a json file.
 */
public class ExampleFileLoader extends ExampleLoader {

    private static final String exampleConfigPathKey = "exampleConfigurationLocation";
    private HashMap<String, JsonObject> examples = new HashMap<>();
    private HashMap<String, String> exampleNames = new HashMap<>();

    @Override
    public Map<String, String> getExampleNames() {
        if (exampleNames.size() == 0) {
            if (examples.size() == 0)
                loadExamples();

            for (String key : this.examples.keySet()) {
                exampleNames.put(examples.get(key).get("class").getAsString(), examples.get(key).get("name").getAsString());
            }
        }

        return exampleNames;
    }

    @Override
    public String getExamplePath(String name) {
        return "/" + name.replaceAll("\\.", "/") + ".scala";
    }

    @Override
    public Namespace getParameters(String className) {
        if (examples.size() == 0)
            loadExamples();

        HashMap<String, Object> attributes = new HashMap<>();

        for (Map.Entry<String, JsonElement> parameter : this.examples.get(className).get("parameter").getAsJsonObject().entrySet()) {
            this.extractParameter(attributes, parameter);
        }

        return new Namespace(attributes);
    }

    private void extractParameter(HashMap<String, Object> attributes, Map.Entry<String, JsonElement> parameter) {
        String value = parameter.getValue().getAsString();

        Pattern p = Pattern.compile("^\\d+$");
        Matcher m = p.matcher(value);
        if (m.matches()) {
            attributes.put(parameter.getKey(), parameter.getValue().getAsInt());
            return;
        }

        p = Pattern.compile("^\\d+\\.\\d+$");
        m = p.matcher(value);
        if (m.matches()) {
            attributes.put(parameter.getKey(), parameter.getValue().getAsDouble());
            return;
        }

        p = Pattern.compile("true|false");
        m = p.matcher(value);
        if (m.matches()) {
            attributes.put(parameter.getKey(), parameter.getValue().getAsBoolean());
            return;
        }

        attributes.put(parameter.getKey(), parameter.getValue().getAsString());
    }

    @Override
    public JsonObject loadComprehensionBoxes(String className) {
        if (examples.size() == 0)
            loadExamples();

        if (this.examples.get(className).get("comprehensions") != null)
            return this.examples.get(className).get("comprehensions").getAsJsonObject();

        return null;
    }

    private void loadExamples() {
        examples = new HashMap<>();
        String examplesPath = ConfigReader.getString(exampleConfigPathKey);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Gson gson = new Gson();


        try (BufferedReader br = new BufferedReader(new InputStreamReader(cl.getResourceAsStream(examplesPath)))){
            JsonArray examples = gson.fromJson(br, JsonArray.class);

            for (JsonElement example : examples) {
                this.examples.put(example.getAsJsonObject().get("class").getAsString(), example.getAsJsonObject());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
