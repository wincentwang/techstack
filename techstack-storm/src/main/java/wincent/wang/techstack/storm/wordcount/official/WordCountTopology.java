package wincent.wang.techstack.storm.wordcount.official;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by user on 7/5/2017.
 */
public class WordCountTopology {

    public static void main(String[] args) throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), Integer.valueOf(5));

        builder.setBolt("split", new SplitSentence(), Integer.valueOf(8)).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), Integer.valueOf(12)).fieldsGrouping("split", new Fields(new String[] { "word" }));

        Config conf = new Config();
        conf.setDebug(true);

        if ((args != null) && (args.length > 0)) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000L);
            cluster.shutdown();
        }
    }

    public static class WordCount extends BaseBasicBolt
    {
        Map<String, Integer> counts = new HashMap();

        public void execute(Tuple tuple, BasicOutputCollector collector)
        {
            String word = tuple.getString(0);
            Integer count = (Integer)this.counts.get(word);
            if (count == null)
                count = Integer.valueOf(0);
            Integer localInteger1 = count; Integer localInteger2 = count = Integer.valueOf(count.intValue() + 1);
            this.counts.put(word, count);
            collector.emit(new Values(new Object[] { word, count }));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {
            declarer.declare(new Fields(new String[] { "word", "count" }));
        }
    }

    public static class SplitSentence extends ShellBolt
            implements IRichBolt
    {
        public SplitSentence()
        {
            super();
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {
            declarer.declare(new Fields(new String[] { "word" }));
        }

        public Map<String, Object> getComponentConfiguration()
        {
            return null;
        }
    }
}
