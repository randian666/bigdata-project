package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
/**
 * @author liuxun
 * @version V1.0
 * @Description: Spark Streaming
 * @date 2017/11/23
 */
public class StreamingTest {

    public static void main(String[] args) {
        try {
            //创建spark context
            SparkConf conf=new SparkConf().setMaster("local").setAppName("Simple App");
            JavaSparkContext sc = new JavaSparkContext(conf);
            // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
            JavaStreamingContext jssc = new JavaStreamingContext(sc, Duration.apply(500));
            JavaDStream<String> dstream = jssc.textFileStream("/export/servers/data/pv.txt");
            dstream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                    stringJavaRDD.foreachPartition(line-> System.out.println(line));
                }
            });
            // 以端口7777作为输入来源创建DStream
//            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 7777);
            // 从DStream中筛选出包含字符串"error"的行
//            JavaDStream<String> errorLines = lines.filter(line -> line.contains("cookie"));
//            errorLines.print();
            // 启动流计算环境StreamingContext并等待它"完成"
            jssc.start();
            // 等待作业完成
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
