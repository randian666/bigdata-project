package com.flink.demo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * flink入门程序
 * @Author: liuxun
 * @CreateDate: 2018/6/18 下午2:50
 * @Version: 1.0
 */
public class DemoScoketFlink {
    private static final Logger LOG = LoggerFactory.getLogger(DemoScoketFlink.class);

    public static void main(String[] args) throws Exception {
//        ParameterTool params = ParameterTool.fromArgs(args);//解析入参
        //获取运行时环境
      LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint机制，确保精确处理一次
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //本地环境请在终端执行 nc -lk 7777模拟消息源输入日志
        DataStream<Tuple2<String,Integer>> dataStream = env
                .socketTextStream("localhost", 7777)//接收端口7777发送过来的日志
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return StringUtils.isNotBlank(s);
                    }
                }).setParallelism(1)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                        LOG.info("接收到日志数据为："+s);
                        for (String word:s.split(" ")){
                            collector.collect(new Tuple2<>(word,1));
                        }
                    }
                })
                //根据第一个字段分组
                .keyBy(0)
                //每隔5秒收集数据流
                .timeWindow(Time.seconds(5))
                //5秒内接受到的数据进行处理
                //通过reduce聚合
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
//                        return new Tuple2(a.f0,a.f1+b.f1);
//                    }
//                });
        //通过sum函数聚合
                .sum(1);
        //通过apply方法聚合
//                .apply(new RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Map<String,Integer> currMap=new HashMap();
//                        for (Tuple2<String,Integer> in:input){
//                            if (!currMap.containsKey(in.f0)){
//                                currMap.put(in.f0,0);
//                            }
//                            currMap.put(in.f0,currMap.get(in.f0)+in.f1);
//                        }
//                        for (Map.Entry entry:currMap.entrySet()){
//                            out.collect(new Tuple2<String, Integer>(entry.getKey().toString(),Integer.parseInt(entry.getValue().toString())));
//                        }
//                    }
//                });
        //数据存储
//        dataStream.addSink(new CsvSink("sink1")).setParallelism(1).name("sink result1");
        dataStream.addSink(new CsvSink("sink2")).setParallelism(1).name("sink result2");
        dataStream.print();
        System.out.println(env.getExecutionPlan());
        //Flink程序是延迟计算。只有执行了execute才真正触发执行。
        env.execute("window WordCount");
    }
}
