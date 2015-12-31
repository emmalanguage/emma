package org.emma.data.code;

import com.google.gson.JsonObject;

public class Example {
    String code = "";
    JsonObject comprehensions = new JsonObject();

    public Example(String code) {
        this.code = code;
    }

    public Example(String code, JsonObject comprehensions) {
        this.code = code;
        this.comprehensions = comprehensions;
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
}
