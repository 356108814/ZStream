package com.ztesoft.zstream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Properties文件加载工具类
 *
 * @author Yuri
 */
public class PropertyUtil {

    private static final Log LOG = LogFactory.getLog(PropertyUtil.class);

    /**
     * 获取配置
     *
     * @return Properties
     */
    public static Properties getProperties(String filename) {
        Properties properties = null;
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(filename);
            LOG.info("加载外部配置" + filename);
        } catch (IOException e) {
            inputStream = PropertyUtil.class.getClassLoader().getResourceAsStream(filename);
            LOG.info("加载jar包内的配置" + filename);
        }
        if (inputStream != null) {
            try {
                properties = new Properties();
                properties.load(inputStream);
            } catch (IOException e) {
                LOG.warn("load properties exception: " + e.getMessage());
            }
        }
        return properties;
    }

    public static File getPropertyFile(String propertyName) {
        File file = null;
        final URL resource = PropertyUtil.class.getClassLoader().getResource(propertyName);
        if (resource != null) {
            file = new File(resource.getFile());
        } else {
            LOG.error("Resource not found: " + propertyName);
        }
        return file;
    }

    /**
     * 动态设置类配置属性
     *
     * @param clazz    要设置的类
     * @param filename 当前类路径下配置文件名称
     */
    public static void setConfigFromFile(Class<?> clazz, String filename) {
        Properties properties = getProperties(filename);
        if (properties != null) {
            Enumeration nameEnum = properties.propertyNames();
            while (nameEnum.hasMoreElements()) {
                String name = (String) nameEnum.nextElement();
                String value = properties.getProperty(name);
                reflectSetProperty(clazz, name, value);
            }
        }
    }

    /**
     * 反射设置类属性
     *
     * @param clazz 要设置的类
     * @param name  属性名称
     * @param value 属性值
     */
    private static void reflectSetProperty(Class<?> clazz, String name, String value) {
        Field field = findField(clazz, name);
        if (field == null) {
            LOG.info("属性[" + name + "]没有定义或者不可修改,忽略处理");
            return;
        }
        Class<?> type = field.getType();
        try {
            if (type == String.class) {
                field.set(clazz, value);
            } else if (type == boolean.class) {
                field.setBoolean(clazz, Boolean.valueOf(value));
            } else if (type == int.class) {
                field.setInt(clazz, Integer.valueOf(value));
            } else if (type == long.class) {
                field.setLong(clazz, Long.valueOf(value));
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    /**
     * 获取类可修改的属性
     *
     * @param clazz clazz
     * @param name  属性名称
     * @return Field
     */
    private static Field findField(Class<?> clazz, String name) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (name.equals(field.getName())) {
                if (Modifier.toString(field.getModifiers()).indexOf("final") > 0) {
                    return null;
                }
                return field;
            }
        }
        return null;
    }
}
