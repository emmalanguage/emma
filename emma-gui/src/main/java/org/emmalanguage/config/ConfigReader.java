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
package org.emmalanguage.config;

import java.io.*;
import java.util.Properties;

public class ConfigReader {

    private static Properties props = null;
    private static final String configLocation = "/config.properties";

    static {

        try {
            props = new Properties();
            InputStream stream = ConfigReader.class.getResourceAsStream(configLocation);
            props.load(stream);
            stream.close();
        } catch (Exception e) {
            System.err.println("Cannot load config at location '" + configLocation + "'. Stopping execution.");
            System.exit(1);
        }
    }

    public static String getString(String key) {
        return props.getProperty(key);
    }

    public static int getInt(String key) {
        return Integer.parseInt(props.getProperty(key));
    }
}
