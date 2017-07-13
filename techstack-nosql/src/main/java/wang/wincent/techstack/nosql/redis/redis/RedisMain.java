package wang.wincent.techstack.nosql.redis.redis;

import redis.clients.jedis.Jedis;

public class RedisMain {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1",6379);
        String response = jedis.ping();
        System.out.println(response);
    }

}
