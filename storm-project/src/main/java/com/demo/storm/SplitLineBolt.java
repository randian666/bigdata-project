package com.demo.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 对文本行进行分割SplitLineBolt
 * @date 2017/11/27
 */
public class SplitLineBolt extends BaseBasicBolt {

    /**
     * 初始化函数，相当于spout中的open
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    /**
     * 这个函数也会被不断执行，但它的数据来自于上游。
     * 这里将文本行分割为单词，并发送
     * @param tuple
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = tuple.getStringByField("line");
        String[] words = line.split(" ");
        for (String word:words){
            word=word.trim();
            if (StringUtils.isBlank(word)){
                continue;
            }
            basicOutputCollector.emit(new Values(word));
        }
    }
    /**
     * 声明发送的数据项，与上面的函数对应。
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }


}
