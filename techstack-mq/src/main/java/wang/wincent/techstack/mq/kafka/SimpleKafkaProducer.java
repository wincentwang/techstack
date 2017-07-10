package wang.wincent.techstack.mq.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by user on 7/7/2017.
 */
public class SimpleKafkaProducer {


    public static void main(String[] args) {
        /**
         * 1、指定当前kafka producer生产的数据的目的地
         *  创建topic可以输入以下命令，在kafka集群的任一节点进行创建。
         *  bin/kafka-topics.sh --create --zookeeper zk01:2181 --replication-factor 1 --partitions 1 --topic test
         */
        String TOPIC = "Wincent";
        /**
         * 2、读取配置文件
         */
        Properties props = new Properties();
        /*
         * key.serializer.class默认为serializer.class
		 */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        /*
		 * kafka broker对应的主机，格式为host1:port1,host2:port2
		 */
        props.put("metadata.broker.list", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
        /*
         * request.required.acks,设置发送数据是否需要服务端的反馈,有三个值0,1,-1
		 * 0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。
		 * 这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
		 * 1，意味着在leader replica已经接收到数据后，producer会得到一个ack。
		 * 这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。
		 * 如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
		 * -1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。
		 * 这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失
		 */
        props.put("request.required.acks", "1");
        /*
		 * 可选配置，如果不配置，则使用默认的partitioner partitioner.class
		 * 默认值：kafka.producer.DefaultPartitioner
		 * 用来把消息分到各个partition中，默认行为是对key进行hash。
		 */
        props.put("partitioner.class", "wang.wincent.techstack.mq.kafka.MyLogPartitioner");
//        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        /**
         * 3、通过配置文件，创建生产者
         */
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        /**
         * 4、通过for循环生产数据
         */
        for (int messageNo = 1; messageNo < 100000; messageNo++) {
            String messageStr = new String(messageNo + "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "用来配合自定义的MyLogPartitioner进行数据分发");

            /**
             * 5、调用producer的send方法发送数据
             * 注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发
             */
            producer.send(new KeyedMessage<String, String>(TOPIC, messageNo + "", "appid" + UUID.randomUUID() + messageStr));
        }
    }


}

