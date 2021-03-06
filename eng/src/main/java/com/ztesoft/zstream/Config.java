package com.ztesoft.zstream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;

/**
 * 配置
 *
 * @author Yuri
 */
public class Config {
    private static final Log LOG = LogFactory.getLog(Config.class);

    public static Properties properties;
    public static String checkpoint;
    public static boolean isUseKerberos = false;
    public static boolean isDebug = false;

    public static void load(String filename) {
        //改成调用java的，不调用Scala。如果调用Scala，则导致maven打包eng的时候不能编译Scala代码，原因未知
        //如果maven编译的时候加上scala:compile，编译可以通过，但是编译的jar包有问题，报错不支持数据源file
        if (filename == null || filename.isEmpty()) {
            filename = "config.properties";
        }
        properties = PropertyUtil.getProperties(filename);
        checkpoint = properties.getProperty("checkpoint");
        isUseKerberos = Boolean.parseBoolean(properties.getProperty("isUseKerberos"));
        isDebug = Boolean.parseBoolean(properties.getProperty("isDebug"));
        LOG.info(String.format("%s配置内容如下：\n%s", filename, properties));
//        PropertyUtil.setConfigFromFile(Config.class, filename);
    }
}
