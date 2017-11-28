package com.demo.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * @author liuxun
 * @version V1.0
 * @Description: Storm计数器
 * @date 2017/11/24
 */
public class TopologyCountTest {
    public static void main(String[] args) {
        // 组建拓扑，并使用流分组
        TopologyBuilder builder=new TopologyBuilder();
        //使用一个线程发送数据
        builder.setSpout("ReadLineSpout",new ReadLineSpout(),1);
        //接受ReadLineSpout的数据
        builder.setBolt("SplitLineBolt",new SplitLineBolt(),2).shuffleGrouping("ReadLineSpout");
        builder.setBolt("CountWordBolt",new CountWordBolt(),2).fieldsGrouping("SplitLineBolt",new Fields("word"));
        builder.setBolt("ReportWordBolt",new ReportWordBolt(),2).globalGrouping("CountWordBolt");
        Config config=new Config();
        config.setDebug(false);
        config.setNumWorkers(1);
        //自定义输入模式1、输出完整的数据2、随机输出
        config.put("type","1");
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("TopologyCountTest",config,builder.createTopology());
        // 休眠4秒，Utils.sleep不会抛出受干扰异常，尤其在Spout休眠时，集群突然shutdown
        Utils.sleep(4000);
        localCluster.killTopology("TopologyCountTest");
        //手动关闭本地集群
        localCluster.shutdown();



    }
}
