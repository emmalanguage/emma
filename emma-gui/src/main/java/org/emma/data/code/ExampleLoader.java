package org.emma.data.code;

import com.google.gson.JsonObject;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

        InputStream is = Thread.currentThread().getClass().getResourceAsStream(filePath);

        if (is == null)
            throw new FileNotFoundException(filePath + " not found.");

        InputStreamReader isr = new InputStreamReader(is);

        char[] buffer = new char[10000];
        try {
            while (isr.read(buffer) != -1) {
                sourceCode.append(new String(buffer));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sourceCode.toString();
    }

    public abstract JsonObject loadComprehensionBoxes(String className);
}
