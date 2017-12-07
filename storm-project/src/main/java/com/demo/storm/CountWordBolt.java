package com.demo.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 对单词进行计数CountWordBolt
 * @date 2017/11/27
 */
public class CountWordBolt extends BaseBasicBolt{
    // 保存数据，生产环境下应保存到数据库中
    private HashMap<String, Integer> countWords;
    /**
     * 初始化函数，相当于spout中的open
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        countWords=new HashMap<String, Integer>();
    }
    /*
      传递两个数据项:一个单词一个单词的数量
    */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("word");
        int count=1;
        if (countWords.containsKey(word)){
            count=countWords.get(word)+1;
        }
        countWords.put(word,count);
        basicOutputCollector.emit(new Values(word,count));
    }
}
