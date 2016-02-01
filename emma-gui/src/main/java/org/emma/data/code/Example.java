package org.emma.data.code;

import com.google.gson.JsonObject;
import net.sourceforge.argparse4j.inf.Namespace;

public class Example {
    String code = "";
    JsonObject comprehensions = new JsonObject();
    Namespace parameters;

    public Example(String code) {
        this.code = code;
    }

    public Example(String code, JsonObject comprehensions) {
        this(code);
        this.comprehensions = comprehensions;
    }

    public Example(String code, JsonObject comprehensions, Namespace parameters) {
        this(code,comprehensions);
        this.setParameters(parameters);
    }

    public String getCode() {
        return code;
    }

    public Example setCode(String code) {
        this.code = code;
        return this;
    }

    public JsonObject getComprehensions() {
        return comprehensions;
    }

    public Namespace getParameters() {
        return parameters;
    }

    public void setParameters(Namespace parameters) {
        this.parameters = parameters;
    }
}
