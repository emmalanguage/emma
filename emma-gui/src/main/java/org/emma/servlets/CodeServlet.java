package org.emma.servlets;

import com.google.gson.Gson;
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

/**
 * Created by Andi on 05.08.2015.
 */
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

        if (!sourceCode.isEmpty())
            return new Example(sourceCode, exampleLoader.loadComprehensionBoxes(exampleName));

        return null;
    }
}
