package wang.wincent.techstack.storm.example.logMonitor.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import wang.wincent.techstack.storm.example.logMonitor.domain.Record;
import wang.wincent.techstack.storm.example.logMonitor.utils.MonitorHandler;

/**
 * Describe: 请补充类描述
 * Data:     2015/11/11.
 */
public class SaveMessage2MySql extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(SaveMessage2MySql.class);
    public void execute(Tuple input, BasicOutputCollector collector) {
        Record record = (Record) input.getValueByField("record");
        MonitorHandler.save(record);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

