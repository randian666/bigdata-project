package com.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

/**
 * @Author: liuxun
 * @CreateDate: 2018/12/24 下午7:04
 * @Version: 1.0
 */
public class DemoFlink {
    public static void main(String[] args) {
        //LocalEnvironment 还可以将自定义配置传递给 Flink。
        Configuration conf = new Configuration();
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
        DataSet<String> data = env.readTextFile("/Users/liuxun/Documents/开始加载日志");
        data.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.contains("info");
                    }
                }).setParallelism(2)
                .writeAsText("/Users/liuxun/Documents/result");
        try {
            JobExecutionResult res = env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
