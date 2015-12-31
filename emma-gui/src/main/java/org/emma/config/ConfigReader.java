package org.emma.config;

import java.io.*;
import java.net.URL;
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
