package com.demo.storm;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * @author liuxun
 * @version V1.0
 * @Description: Trident进行单词计数
 * @date 2017/12/7
 */
public class TridentTest {
    public static void main(String[] args) {
        FixedBatchSpout spout=new FixedBatchSpout(new Fields("line"),3,new Values("hello my name is liuxun"),new Values("hello my name is zhangyun"));
        spout.setCycle(true);
        TridentTopology topology=new TridentTopology();
        TridentState wordCounts=topology.newStream("spout1",spout)
                .each(new Fields("line"),new Split(),new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"))
                .parallelismHint(6);


    }
}
