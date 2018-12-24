package com.spark.demo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
/**
 * spark本地模式分析本地文件
 * Created by liuxun on 2017/10/17.
 */
public class SimpleApp {
    public static void main(String[] args) {
        //bin/spark-submit --class "com.spark.demo.SimpleApp" /export/servers/data/spark-project-1.0-SNAPSHOT.jar 2>&1 | grep "Lines with a"
        String logFile = "/Users/liuxun/gitwork/bigdata-project/spark-project/src/main/resources/abc.txt"; // Should be some file on your system
        //本地运行
        SparkConf conf=new SparkConf().setMaster("local").setAppName("Simple App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //线上运行
//        JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
//                "file:///export/servers/spark/", new String[]{"target/spark-project-1.0-SNAPSHOT.jar"});
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
