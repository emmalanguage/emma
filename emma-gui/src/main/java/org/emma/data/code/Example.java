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
