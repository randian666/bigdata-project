package com.demo.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * @author liuxun
 * @version V1.0
 * @Description: 用于读取数据，并将数据往下传递，是数据的源头。
 * @date 2017/11/27
 */
public class ReadLineSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private Map conf;
    private int type;//数据发送类型
    private ArrayList<String> arrayList;
    private boolean finished;
    private final String[] lines = {
            "long long ago I like playing with cat",
            "playing with cat make me happy",
            "I feel happy to be with you",
            "you give me courage",
            "I like to be together with you",
            "long long ago I like you"
    };

    /**
     * 声明发送的元祖tuple，必须与Values中的元素个数对应，在下一个bolt中可以通过对应的名字或者所在数字顺序获得这个Tuple
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    /**
     * 相当于构造函数，做一些初始化工作。
     * @param conf  map可以获取配置信息，这些配置信息在TopologyBuilder时被设置好。
     * @param topologyContext
     * @param spoutOutputCollector   用于传递Tuple
     */
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector=collector;
        this.conf=conf;
        this.type=Integer.parseInt(conf.get("type").toString());
        arrayList=new ArrayList<String>();
        for (String line:lines){
            arrayList.add(line);
        }
    }
    /**
     * 这个函数或被不断调用，这里通过随机数的方式不断发送文本行，模拟流数据。
     */
    public void nextTuple() {
        if(type == 1){
            emitOnce();
        }
        else{
            emitRandomAndContinue();
        }
    }
    /*
     type 1
     只发送一次，且顺序发送
   */
    private void emitOnce(){
        if(finished){
            Utils.sleep(100);
            return;
        }

        for(String line : lines){
            this.collector.emit(new Values(line),System.currentTimeMillis());
        }
        finished = true;
    }
    /*
     type 2
     不断随机发送
    */
    private void emitRandomAndContinue(){
        Random random = new Random();
        int id = random.nextInt();
        id = id < 0? -id : id;
        id = id % arrayList.size();
        this.collector.emit(new Values(arrayList.get(id)));
    }
    @Override
    public void fail(Object msgId) {
        System.out.println("fail message {} "+msgId);
        super.fail(msgId);
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("ack message {} "+msgId);
        super.ack(msgId);
    }
}

