package com.spark.demo;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
//        pairRdd.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                System.out.println("pairRdd data:"+stringStringTuple2);
//            }
//        });
        //使用parallelizePairs生成 pairsRdd
        Tuple2 t1 = new Tuple2(1, 2);
        Tuple2 t2 = new Tuple2(3, 4);
        Tuple2 t3 = new Tuple2(3, 6);
        List<Tuple2<Integer, Integer>> listPairs=Arrays.asList(t1,t2,t3);
        JavaPairRDD<Integer, Integer> lines1 = sc.parallelizePairs(listPairs);
        System.out.println("parallelizePairs lines1 data:"+ StringUtils.join(lines1.collect(),","));
        System.out.println("------------------------------------------------------");
        //删掉RDD中键与other  RDD 中的键相同的元素
        Tuple2 t4 = new Tuple2(3, 9);
        Tuple2 t5 = new Tuple2(4, 6);
        List<Tuple2<Integer, Integer>> list2=Arrays.asList(t4,t5);
        JavaPairRDD<Integer, Integer> lines2 = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, Integer> subRdd = lines1.subtractByKey(lines2);
        System.out.println("subtractByKey subRdd data:"+ StringUtils.join(subRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //对两个RDD进行内连接
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinRdd = lines1.join(lines2);
        System.out.println("join joinRdd data:"+ StringUtils.join(joinRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //对两个RDD进行连接操作，确保第二个RDD的键必须存在（左外连接）
        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> leftRdd = lines1.leftOuterJoin(lines2);
        System.out.println("leftOuterJoin leftRdd data:"+ StringUtils.join(leftRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //对两个RDD进行连接操作，确保第一个RDD的键必须存在（右外连接）
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightRdd = lines1.rightOuterJoin(lines2);
        System.out.println("rightOuterJoin rightRdd data:"+ StringUtils.join(rightRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //将两个RDD中拥有相同键的数据分组到一起
        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupRdd = lines1.cogroup(lines2);
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
        JavaPairRDD<String, String> filterRdd = pairRdd.filter((Function<Tuple2<String, String>, Boolean>) stringStringTuple2 -> stringStringTuple2._2().length() >7);
        System.out.println("filterRdd data:"+ StringUtils.join(filterRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //根据键排序
        pairRdd.sortByKey(true);
        System.out.println("pairRdd sort data:"+ StringUtils.join(pairRdd.collect(),","));
        System.out.println("------------------------------------------------------");
        //返回给定键对应的所有值
        List<String> list = pairRdd.lookup("1");
        System.out.println("pairRdd list data:"+ StringUtils.join(list,","));
        System.out.println("------------------------------------------------------");
        //combineByKey求每个建对应的平均值
        Function<Integer, AvgCount> createCombiner =x->new AvgCount(x, 1);
        Function2<AvgCount, Integer, AvgCount> mergeValue =(a, x) -> {
            a.total +=x;
            a.num+=1;
            return a;
        };
        Function2<AvgCount, AvgCount, AvgCount> mergeCombiners =(a, b) -> {
            a.total += b.total;
            a.num += b.num;
            return a;
        };
        //createCombiner()的函数来创建那个键对应的累加器的初始值。需要注意的是，这一过程会在每个分区中第一次出现各个,键时发生，而不是在整个RDD中第一次出现一个键时发生
        //如果这是一个在处理当前分区之前已经遇到的键，它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
        //由于每个分区都是独立处理的，因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器，就需要使用用户提供的mergeCombiners()方法将各个分区的结果进行合并
        JavaPairRDD combineRdd = lines1.combineByKey(createCombiner,mergeValue,mergeCombiners);
        Map<Integer,AvgCount> map = combineRdd.collectAsMap();
        Gson gson=new Gson();
        System.out.println("combineByKey combineRdd data:"+gson.toJson(map));
        for(Map.Entry<Integer,AvgCount> e:map.entrySet()){
            System.out.println("combineByKey avg data:"+e.getKey()+"="+e.getValue().avg());
        }
    }

    /**
     * 每个建对应的平均值
     */
    public static class AvgCount implements Serializable{
        public int total;//相同键对应的总值
        public int num;//相同键对应的个数
        public AvgCount(int total,int num){
            this.total=total;
            this.num=num;
        }
        public float avg(){
            return total/num;
        }
    }
}
