package com.demo.storm.logreader;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 输入结果
 * @date 2017/12/8
 */
public class LogWrite extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String user = tuple.getStringByField("user");
        Integer pv=tuple.getIntegerByField("pv");
        System.out.println(String.format("************************************%s:%d",user,pv));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
