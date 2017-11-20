package com.ztesoft.zstream;

import java.util.Enumeration;
import java.util.Properties;

/**
 * 配置
 *
 * @author Yuri
 */
public class Config {
    public static String checkpoint;
    public static boolean isUseKerberos = false;
    public static boolean isDebug = false;

    public static void load(String filename) {
        if (filename == null || filename.isEmpty()) {
            filename = "config.properties";
        }
        Properties properties = PropertyUtil.getProperties(filename);
        if (properties != null) {
            Enumeration nameEnum = properties.propertyNames();
            while (nameEnum.hasMoreElements()) {
                String name = (String) nameEnum.nextElement();
                String value = properties.getProperty(name);
                PropertyUtil.reflectSetProperty(Config.class, name, value);
            }
        }
    }
}
