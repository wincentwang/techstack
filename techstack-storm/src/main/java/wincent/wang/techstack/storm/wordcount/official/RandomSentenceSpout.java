package wincent.wang.techstack.storm.wordcount.official;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by user on 7/5/2017.
 */
public class RandomSentenceSpout  extends BaseRichSpout {


    SpoutOutputCollector _collector;
    Random _rand;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this._collector = collector;
        this._rand = new Random();
    }

    public void nextTuple()
    {
        Utils.sleep(100L);
        String[] sentences = { "the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String sentence = sentences[this._rand.nextInt(sentences.length)];
        this._collector.emit(new Values(new Object[] { sentence }));
    }

    public void ack(Object id)
    {
    }

    public void fail(Object id)
    {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(new String[] { "word" }));
    }

}
