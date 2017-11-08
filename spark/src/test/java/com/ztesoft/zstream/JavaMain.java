package com.ztesoft.zstream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @author Yuri
 */
public class JavaMain {
    public static void main(String[] args) {
        String filePath = "J:/spark/source/user.txt";
        if(args.length != 0) {
            filePath = args[0];
        }
        SparkConf conf = new SparkConf();
        conf.setAppName("main").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(filePath);
        lines.foreach((VoidFunction<String>) s -> System.out.println(s));
    }
}
