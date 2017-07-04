package wang.wincent.techstack.storm.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by user on 6/28/2017.
 */
public class MySpiltBolt extends BaseRichBolt {

    OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    //被storm框架while(true)循环调用tuple
    public void execute(Tuple tuple) {
        String lines=tuple.getString(0);
        String[] arrs=lines.split(" ");
        for (String arr:arrs) {
            outputCollector.emit(new Values(arr,1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","num"));
    }



}
