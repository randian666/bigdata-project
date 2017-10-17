package com.spark.demo;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
/**
 * Created by liuxun on 2017/10/17.
 */
public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "file:///export/servers/spark/README.md"; // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
                "file:///export/servers/spark/", new String[]{"target/spark-project-1.0-SNAPSHOT.jar"});
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
