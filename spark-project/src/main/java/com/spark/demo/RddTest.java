package com.spark.demo;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * @author liuxun
 * @version V1.0
 * @Description: RDD编程
 * @date 2017/10/31
 */
public class RddTest {
    public static void main(String[] args) {
        //创建本地环境spark context
        SparkConf conf=new SparkConf().setMaster("local").setAppName("RddTest");
        JavaSparkContext sc=new JavaSparkContext(conf);
        /**
         * 1、创建RDD
         * 创建RDD有两种方式，一种是读取外部文件，一种是利用已有的集合进行并行化,
         */
        JavaRDD<Integer> rddList = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        JavaRDD<String> rddFile = sc.textFile("file:///export/servers/spark/README.md");
        System.out.println(rddList.count());
        System.out.println(rddFile.count());
        /**
         * 2、转化操作
         * 转化操作就是从已有RDD中创建出新的RDD
         */
//        JavaRDD<String> sparakRdd = rddFile.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("spark");
//            }
//        });
        //转化操作filter()则接收一个函数，并将RDD中满足该函数的元素放入新的RDD中返回
        JavaRDD<String> sparakRdd=rddFile.filter(s -> s.contains("spark")); //java8匿名函数写法
        JavaRDD<String> javaRdd=rddFile.filter(s -> s.contains("java"));
        //转化操作map() 接收一个函数，把这个函数用于RDD中的每个元素，将函数的返回结果作为结果RDD中对应元素的值
        JavaRDD<Integer> rddResult = rddList.map(s -> s * s);//计算RDD中的所有元素的平方，适合数字类型，返回一个输出值
        System.out.println("rddResult:"+StringUtils.join(rddResult.collect(),","));
        JavaRDD<String> stringRdd = sc.parallelize(Arrays.asList("Hello liuxun", "hello zhangsan"));//将RDD中的元素切分单词，返回多个输出值
        JavaRDD<String> newStringRdd = stringRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println("newStringRdd:"+StringUtils.join(newStringRdd.collect(),","));
        /**
         * 3、行动操作
         * 行动操作就是操作RDD并返回结果或者写入外部系统的操作，会出发实际的计算。
         */
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
        //笛卡尔积
        JavaPairRDD<String, String> cartRdd = stringRdd.cartesian(sparakRdd);
        System.out.println("cartRdd:"+StringUtils.join(cartRdd.collect(),","));
        //返回一个由只存在于第一个RDD中而不在第二个RDD中的所有元素
        JavaRDD<String> subRdd = stringRdd.subtract(sparakRdd);
        System.out.println("返回一个由只存在于第一个RDD中而不在第二个RDD中的所有元素:"+StringUtils.join(subRdd.collect(),","));
        //返回两个RDD中都有的元素
        JavaRDD<String> interRdd = stringRdd.intersection(sparakRdd);
        System.out.println("返回两个RDD中都有的元素:"+StringUtils.join(interRdd.collect(),","));
        //类型转换
        JavaDoubleRDD doubleRdd = rddList.mapToDouble(x -> x * x);
        System.out.println(doubleRdd.mean());
        //把数据以序列化的形式缓存在JVM的堆空间中
        JavaDoubleRDD storeRdd=doubleRdd.persist(StorageLevel.DISK_ONLY());
        System.out.println("storeRdd count:"+storeRdd.count());
        System.out.println("storeRdd data:"+StringUtils.join(storeRdd.collect(),","));
    }
}
