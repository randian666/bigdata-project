package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 创建RDD有两种方式，一种是读取外部文件，一种是利用已有的集合进行并行化,转化操作就是从已有RDD中创建出新的RDD
 * 行动操作就是操作RDD并返回结果或者写入外部系统的操作，会出发实际的计算。
 * @date 2017/10/31
 */
public class RddTest {
    public static void main(String[] args) {
        //创建spark context
        SparkConf conf=new SparkConf().setMaster("local").setAppName("RddTest");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddList = sc.parallelize(Arrays.asList("pandas","i like pandas"));
        JavaRDD<String> rddFile = sc.textFile("file:///export/servers/spark/README.md");
        System.out.println(rddList.count());
        System.out.println(rddFile.count());
        //转化操作
//        JavaRDD<String> sparakRdd = rddFile.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("spark");
//            }
//        });
        //java8匿名函数写法
        JavaRDD<String> sparakRdd=rddFile.filter(s -> s.contains("spark"));
        JavaRDD<String> javaRdd=rddFile.filter(s -> s.contains("java"));
        //行动操作
        for (String line:sparakRdd.take((int)sparakRdd.count())){
            System.out.println("spark string:"+line);
        }
        //如果你的程序把RDD筛选到一个很小的规模，并且你想在本地处理这些数据时，就可以使用它。记住，只有当你的整个数据集能在单台机器的内存中放得下
        //时，才能使用collect()，因此，collect() 不能用在大规模数据集上。
        for (String line:sparakRdd.collect()){
            System.out.println("spark all string:"+line);
        }
        //Rdd合并
        JavaRDD<String> newRdd = sparakRdd.union(javaRdd);
        System.out.println("sparakRdd count:"+sparakRdd.count()+",javaRdd count:"+javaRdd.count());
        System.out.println("newRdd count:"+newRdd.count());

    }
}
