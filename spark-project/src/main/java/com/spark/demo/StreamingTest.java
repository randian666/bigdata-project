package com.spark.demo;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

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
            SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("StreamingTest App");
            JavaSparkContext sc = new JavaSparkContext(conf);
            // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
            JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
//            JavaDStream<String> dstream = jssc.textFileStream("/export/servers/data/pv.txt");
//            dstream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//                @Override
//                public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                    stringJavaRDD.foreachPartition(line-> System.out.println(line));
//                }
//            });
            // 以端口7777作为输入来源创建DStream  终端输入：nc -lk 7777
            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 7777);
            //把一行数据转化成单个单次  以空格分隔
            //拆分行成单词
            JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                public Iterator<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" ")).iterator();
                }
            });

            //调用window函数，生成新的DStream，每隔3秒聚合过去6秒内的源数据，滑动间隔不填默认3秒
            //等价于words.window(Durations.seconds(6),Durations.seconds(6));
            JavaDStream<String> newWords = words.window(Durations.seconds(6));

            //计算每个单词出现的个数
            JavaPairDStream<String, Integer> wordCounts = newWords.mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });

            //输出结果
            wordCounts.print();

            //开始作业
            jssc.start();
            try {
                jssc.awaitTermination();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jssc.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
