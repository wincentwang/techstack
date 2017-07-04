package wang.wincent.techstack.storm.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by user on 6/28/2017.
 */
public class MyCountBolt extends BaseRichBolt{

    OutputCollector outputCollector;

    Map<String,Integer> map=new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    //被storm框架while(true)循环调用tuple
    public void execute(Tuple tuple) {
        String w1=tuple.getString(0);
        Integer w2=tuple.getInteger(1);
        //print thread
        System.out.println(Thread.currentThread().getId()+"word:"+w1);
        if(map.containsKey(w1)){
            Integer count=map.get(w1);
            map.put(w1,++count);
        }else{
            map.put(w1,w2);
        }
       System.out.println(map);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
