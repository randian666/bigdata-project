package com.flink.demo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CsvSink extends RichSinkFunction<Tuple2<String, Integer>> {
    private String name;
    public CsvSink(String name){
       this.name=name;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("CsvSink 初始化");
//        super.open(parameters);
    }
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        if (name.equals("sink1")){
            Thread.sleep(1000);
        }
        System.out.println(name+"result is "+JSON.toJSON(value));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
