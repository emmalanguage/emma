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
package org.emma.servlets;

import com.google.gson.Gson;
import net.sourceforge.argparse4j.inf.Namespace;
import org.emma.data.code.Example;
import org.emma.data.code.ExampleFileLoader;
import org.emma.data.code.ExampleLoader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class CodeServlet extends HttpServlet {

    private ExampleLoader exampleLoader = new ExampleFileLoader();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.mapRequest(req, resp);
    }

    private void mapRequest(HttpServletRequest req, HttpServletResponse resp) {
        String requestName = req.getPathInfo().substring(1, req.getPathInfo().length());

        Gson gson = new Gson();
        String json;
        switch (requestName) {
            case "getExamples":
                ArrayList<Map.Entry<String, String>> exampleClasses = this.getSortedExampleClasses();
                json = gson.toJson(exampleClasses);
                break;
            default: {
                Example example = this.getExample(requestName);
                json = gson.toJson(example);
                break;
            }
        }

        try {
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.addHeader("Content-Type", "application/json");
            resp.getWriter().println(json);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> getExampleClasses() {
        return exampleLoader.getExampleNames();
    }

    private ArrayList<Map.Entry<String, String>> getSortedExampleClasses() {
        ArrayList<Map.Entry<String, String>> classes = new ArrayList<>();

        for (Map.Entry<String, String> entry : this.getExampleClasses().entrySet()) {
            classes.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
        }

        Collections.sort(classes, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        return classes;
    }

    private Example getExample(String exampleName) {
        String filePath = exampleLoader.getExamplePath(exampleName);
        String sourceCode = "";
        try {
            sourceCode = exampleLoader.loadExampleSourceCode(filePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        if (!sourceCode.isEmpty()) {
            Namespace parameters = convertParametersToString(exampleName);
            return new Example(sourceCode, exampleLoader.loadComprehensionBoxes(exampleName), parameters);
        }

        return null;
    }

    private Namespace convertParametersToString(String exampleName) {
        Namespace ns = exampleLoader.getParameters(exampleName);

        HashMap<String, Object> newMap = new HashMap<>();
        Map<String, Object> attrs = ns.getAttrs();
        for (String key : attrs.keySet()) {
            newMap.put(key, attrs.get(key).toString());
        }
        return new Namespace(newMap);
    }
}
