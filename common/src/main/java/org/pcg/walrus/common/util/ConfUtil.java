package org.pcg.walrus.common.util;

import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

public class ConfUtil {

    /**
     * -key_a val_a -key_b val_b => {key_a:val_a, key_b:val_b}
     * @param args
     * @return
     */
    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                map.put(args[i].replace("-", ""), args[++i]);
            }
        }
        return map;
    }

    /**
     * read properties file in class path
     */
    public static Map<String, String> readProperties(String file) throws IOException {
        Map<String, String> properties = new HashMap<>();
        Properties p = new Properties();
        InputStream input = ConfUtil.class.getResourceAsStream(file);
        p.load(input);
        for(String k: p.stringPropertyNames()) properties.put(k, p.getProperty(k));
        return properties;
    }
}
