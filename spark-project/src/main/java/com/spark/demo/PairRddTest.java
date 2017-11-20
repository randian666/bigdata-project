package com.spark.demo;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 键值队RDD操作
 * @date 2017/11/13
 */
public class PairRddTest {
    public static void main(String[] args) {
        //创建sparkContext
        SparkConf sparkConf=new SparkConf().setMaster("local").setAppName("PairRddTest");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        //convert  from other rdd
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("1 Xun", "2 Bai", "3 Yuan Fang", "4 Yun"));
        //每一行的第一个单词作为键，生成一个pair Rdd
        JavaPairRDD<String, String> pairRdd = lines.mapToPair(x->new Tuple2<String, String>(x.split(" ")[0],x));
        //打印结果
        System.out.println("pairRdd data:"+ StringUtils.join(pairRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        pairRdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println("pairRdd data:"+stringStringTuple2);
            }
        });
        //使用parallelizePairs生成 pairsRdd
        Tuple2 t1 = new Tuple2(1, 2);
        Tuple2 t2 = new Tuple2(3, 4);
        Tuple2 t3 = new Tuple2(3, 6);
        List<Tuple2<Object, Object>> listPairs=Arrays.asList(t1,t2,t3);
        JavaPairRDD<Object, Object> lines1 = sc.parallelizePairs(listPairs);
        System.out.println("parallelizePairs lines1 data:"+ StringUtils.join(lines1.collect(),","));
        System.out.println("------------------------------------------------------");
        //删掉RDD中键与other  RDD 中的键相同的元素
        Tuple2 t4 = new Tuple2(3, 9);
        Tuple2 t5 = new Tuple2(4, 6);
        List<Tuple2<Object, Object>> list2=Arrays.asList(t4,t5);
        JavaPairRDD<Object, Object> lines2 = sc.parallelizePairs(list2);
        JavaPairRDD<Object, Object> subRdd = lines1.subtractByKey(lines2);
        System.out.println("subtractByKey subRdd data:"+ StringUtils.join(subRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //对两个RDD进行内连接
        JavaPairRDD<Object, Tuple2<Object, Object>> joinRdd = lines1.join(lines2);
        System.out.println("join joinRdd data:"+ StringUtils.join(joinRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //对两个RDD进行连接操作，确保第二个RDD的键必须存在（左外连接）
        JavaPairRDD<Object, Tuple2<Object, Optional<Object>>> leftRdd = lines1.leftOuterJoin(lines2);
        System.out.println("leftOuterJoin leftRdd data:"+ StringUtils.join(leftRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //对两个RDD进行连接操作，确保第一个RDD的键必须存在（右外连接）
        JavaPairRDD<Object, Tuple2<Optional<Object>, Object>> rightRdd = lines1.rightOuterJoin(lines2);
        System.out.println("rightOuterJoin rightRdd data:"+ StringUtils.join(rightRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //将两个RDD中拥有相同键的数据分组到一起
        JavaPairRDD<Object, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupRdd = lines1.cogroup(lines2);
        System.out.println("cogroup cogroupRdd data:"+ StringUtils.join(cogroupRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //reduceByKey 合并具有相同键的值
        JavaPairRDD<String, String> reduceRdd = pairRdd.reduceByKey((s, s2) -> s + s2);
        System.out.println("reduceByKey reduceRdd data:"+ StringUtils.join(reduceRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //groupByKey 对具有相同键的值进行分组
        JavaPairRDD<String, Iterable<String>> groupRdd = pairRdd.groupByKey();
        System.out.println("groupByKey reduceRdd data:"+ StringUtils.join(groupRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //返回一个仅仅包含键的RDD
        JavaRDD<String> keysRdd = pairRdd.keys();
        System.out.println("keys keysRdd data:"+ StringUtils.join(keysRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //返回一个根据键排序的RDD
        JavaPairRDD<String, String> sortRdd = pairRdd.sortByKey(false);
        System.out.println("sortRdd data:"+ StringUtils.join(sortRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //过滤转化操作 过滤PairRDD值长度大于7的元素
        JavaPairRDD<String, String> filterRdd = pairRdd.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2().length() >7;
            }
        });
        System.out.println("filterRdd data:"+ StringUtils.join(filterRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //根据键排序
        pairRdd.sortByKey(true);
        System.out.println("pairRdd sort data:"+ StringUtils.join(pairRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //返回给定键对应的所有值
        List<String> list = pairRdd.lookup("Liu");
        System.out.println("pairRdd list data:"+ StringUtils.join(list,","));
        System.out.println("------------------------------------------------------");
    }
}
