package com.demo.storm.logreader;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author liuxun
 * @version V1.0
 * @Description: storm分析用户日志，统计pv
 * storm jar /home/hadoop/Downloads/storm-project-1.0-SNAPSHOT.jar "com.demo.storm.logreader.TopologyLogTest"
 * @date 2017/12/8
 */
public class TopologyLogTest {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        // 组建拓扑，并使用流分组
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("LogReader",new LogReader(),1);
        builder.setBolt("LogStat",new LogStat(),2)
                .fieldsGrouping("LogReader","log",new Fields("user"))
                .allGrouping("LogReader","stop");
        builder.setBolt("LogWrite",new LogWrite(),1).shuffleGrouping("LogStat");
        Config config=new Config();
        config.setDebug(false);//关闭调试模式，本地环境可以打开。
        config.setNumWorkers(1);//分配工作进程
        StormSubmitter.submitTopology("TopologyLogTest",config,builder.createTopology());
    }
}
