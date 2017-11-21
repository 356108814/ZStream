package com.ztesoft.zstream;

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
        //改成调用java的，不调用Scala。如果调用Scala，则导致maven打包eng的时候不能编译Scala代码，原因未知
        //如果maven编译的时候加上scala:compile，编译可以通过，但是编译的jar包有问题，报错不支持数据源file
        if (filename == null || filename.isEmpty()) {
            filename = "config.properties";
        }
        PropertyUtil.setConfigFromFile(Config.class, filename);
    }
}
