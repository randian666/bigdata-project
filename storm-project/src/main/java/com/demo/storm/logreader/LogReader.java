package com.demo.storm.logreader;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 日志读取spout组件，负责读取、分发日志。
 * @date 2017/12/8
 */
public class LogReader extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private String[] users={"userA","userB","userC"};
    private String[] urls={"url1","url2","url3"};
    private Random random=new Random();
    private int count=100;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time","user","url"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        while (count-- >0){
            spoutOutputCollector.emit(new Values(System.currentTimeMillis(),users[random.nextInt(3)],urls[random.nextInt(3)]));
        }
    }

}
