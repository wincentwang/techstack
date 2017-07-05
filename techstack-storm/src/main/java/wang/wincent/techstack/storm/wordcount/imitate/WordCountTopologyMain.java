package wang.wincent.techstack.storm.wordcount.imitate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by user on 6/28/2017.
 */

public class WordCountTopologyMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {


        //1，准备一个TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MySpout(), 2);
        topologyBuilder.setBolt("mySplitBolt1", new MySpiltBolt(), 4).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("myCountBolt2", new MyCountBolt(), 2).fieldsGrouping("mySplitBolt1", new Fields("word"));


        //2，创建一个configuration，用来指定当前Topology需要Worker的数量
        Config cfg = new Config();
        cfg.setNumWorkers(2);

        //3，提交任务 两种模式，本地模式和集群模式
        //集群模式
        //StormSubmitter.submitTopology("MyWordCount", cfg, topologyBuilder.createTopology());

        //本地模式
        LocalCluster lc=new LocalCluster();
        lc.submitTopology("MyWordCount",cfg,topologyBuilder.createTopology());

    }
}
