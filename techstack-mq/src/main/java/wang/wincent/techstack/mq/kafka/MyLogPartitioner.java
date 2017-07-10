package wang.wincent.techstack.mq.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;


public class MyLogPartitioner implements Partitioner {
    private static Logger logger = Logger.getLogger(MyLogPartitioner.class);

    public MyLogPartitioner(VerifiableProperties props) {

    }

    public int partition(Object obj, int numPartitions) {
        //return Integer.parseInt(obj.toString())%numPartitions;
        return 1;
    }

}
