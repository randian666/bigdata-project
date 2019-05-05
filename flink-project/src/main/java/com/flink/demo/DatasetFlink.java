package com.flink.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink批处理demo
 */
public class DatasetFlink {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSource<String> datasource = env.readTextFile("D:\\data\\input.txt");
        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");
        DataSet<Tuple2<String, Integer>> dataset = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        dataset.writeAsText("D:\\data\\out"+System.currentTimeMillis()+"").setParallelism(1);
        env.execute("DatasetFlink");
    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (StringUtils.isNoneBlank(token)){
                    collector.collect(new Tuple2<>(token,1));
                }
            }
        }
    }
}
