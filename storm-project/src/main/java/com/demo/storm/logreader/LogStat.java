package com.demo.storm.logreader;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 统计用PV数
 * @date 2017/12/8
 */
public class LogStat extends BaseBasicBolt{
    private Map<String,Integer> pvMap=new HashMap<String,Integer>();
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String streamId=tuple.getSourceStreamId();
//        System.out.println("***********streamId="+streamId);
        if ("log".equals(streamId)){
            String usr=tuple.getStringByField("user");
            if (pvMap.containsKey(usr)){
                pvMap.put(usr,pvMap.get(usr)+1);
            }else {
                pvMap.put(usr,1);
            }
        }
        if ("stop".equals(streamId)){//如果日志读取完毕就一起发送
            Iterator<Map.Entry<String, Integer>> it = pvMap.entrySet().iterator();
            while (it.hasNext()){
                Map.Entry<String, Integer> entry = it.next();
//                System.out.println("***********entry="+entry.getKey()+entry.getValue());
                //输入用户跟pv数
                basicOutputCollector.emit(new Values(entry.getKey(),entry.getValue()));
            }
        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user","pv"));
    }
}
