package org.emma.config;

import java.io.*;
import java.util.Properties;

/**
 * Created by Andi on 04.08.2015.
 */
public class ConfigReader {

    private static Properties props = null;
    private static final String defaultConfigLocation = "src/main/config.properties";

    public static void init(String configPath) {
        props = new Properties();
        BufferedInputStream stream = null;

        try {
            stream = new BufferedInputStream(new FileInputStream(configPath));
        } catch (FileNotFoundException e) {
            System.err.println("'" + configPath + "' not found. Backing up to default configs.");
            File defaultPath = new File(defaultConfigLocation);
            try {
                stream = new BufferedInputStream(new FileInputStream(defaultPath));
            } catch (FileNotFoundException e1) {
                System.err.println("Default config not found at location '" + defaultPath.getAbsolutePath() + "'. Stopping execution.");
                System.exit(0);
            }
        }

        try {
            props.load(stream);
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getString(String key) {
        if (props == null)
            init(defaultConfigLocation);

        return props.getProperty(key);
    }

    public static int getInt(String key) {
        if (props == null)
            init(defaultConfigLocation);

        return Integer.parseInt(props.getProperty(key));
    }
}
