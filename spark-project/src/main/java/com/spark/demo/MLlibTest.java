package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.SVMWithSGD;

import java.util.Arrays;
import java.util.List;

/**
 * @author liuxun
 * @version V1.0
 * @Description: spark机器学习
 * @date 2017/11/23
 */
public class MLlibTest {

    public static void main(String[] args) {
        //创建spark context
        SparkConf conf=new SparkConf().setMaster("local").setAppName("MLlibTest App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SVMWithSGD svmsgd = new SVMWithSGD();


    }
}
