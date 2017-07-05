package wang.wincent.techstack.storm.wordcount.imitate;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by user on 6/28/2017.
 */
public class MySpout extends BaseRichSpout{

    SpoutOutputCollector collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
    }

    //storm 框架在while(true) 调用nextTuple
    public void nextTuple() {
        collector.emit(new Values("Name:Wincent.Wang Webstie:http://wincent.wang"));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Info"));
    }

}
