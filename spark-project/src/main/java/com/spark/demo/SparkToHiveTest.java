package com.spark.demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author liuxun
 * @version V1.0
 * @Description: spark读取hive中的数据
 * @date 2017/11/21
 */
public class SparkToHiveTest {

    public static void main(String[] args) {
        //创建spark context
        SparkSession spark = SparkSession.builder().appName("SparkToHiveTest").master("local").enableHiveSupport().getOrCreate();
        Dataset<Row> data = spark.sql("select * from hive.user_da");
        Row rowFirst = data.first();
        JavaRDD<String> d = data.toJavaRDD().map(line -> line.getString(0));
        System.out.println(d.collect());
        System.out.println("hive rowFirst data"+rowFirst.getString(0));
    }
}
