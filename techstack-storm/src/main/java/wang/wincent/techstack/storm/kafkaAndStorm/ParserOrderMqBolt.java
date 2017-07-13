package wang.wincent.techstack.storm.kafkaAndStorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import wang.wincent.techstack.storm.order.OrderInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by maoxiangyi on 2016/5/4.
 */
public class ParserOrderMqBolt extends BaseRichBolt {
    private JedisPool pool;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //change "maxActive" -> "maxTotal" and "maxWait" -> "maxWaitMillis" in all examples
        JedisPoolConfig config = new JedisPoolConfig();
        //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(5);
        //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setMaxTotal(1000 * 100);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(30);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        /**
         *如果你遇到 java.net.SocketTimeoutException: Read timed out exception的异常信息
         *请尝试在构造JedisPool的时候设置自己的超时值. JedisPool默认的超时时间是2秒(单位毫秒)
         */
        pool = new JedisPool(config, "127.0.0.1", 6379);
    }

    @Override
    public void execute(Tuple input) {
        Jedis jedis = pool.getResource();
        //获取kafkaSpout发送过来的数据，是一个json
        String string = new String((byte[]) input.getValue(0));
        //解析json
        OrderInfo orderInfo = (OrderInfo) new  Gson().fromJson(string, OrderInfo.class);
        //整个网站，各个业务线，各个品类，各个店铺，各个品牌，每个商品
        //获取整个网站的金额统计指标
//        String totalAmount =  jedis.get("totalAmount");
        jedis.incrBy("totalAmount",orderInfo.getProductPrice());
        //获取商品所属业务线的指标信息
        String bid =  getBubyProductId(orderInfo.getProductId(),"b");
//        String bAmout =  jedis.get(bid+"Amout");
        jedis.incrBy(bid+"Amount",orderInfo.getProductPrice());
        jedis.close();
    }

    private String getBubyProductId(String productId,String type) {
//        key:value
        //index:productID:info---->Map
        //  productId-----<各个业务线，各个品类，各个店铺，各个品牌，每个商品>
        Map<String,String> map =  new HashMap<>();
        map.put("b","3c");
        map.put("c","phone");
        map.put("s","121");
        map.put("p","iphone");
        return map.get(type);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
