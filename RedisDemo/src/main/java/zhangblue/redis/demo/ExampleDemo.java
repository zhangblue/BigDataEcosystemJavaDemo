package zhangblue.redis.demo;

import redis.clients.jedis.Jedis;
import zhangblue.redis.repository.RedisResources;

public class ExampleDemo {

  private RedisResources redisResources;

  public ExampleDemo(RedisResources redisResources) {
    this.redisResources = redisResources;
  }

  /**
   * 向redis放入数据
   */
  public void setKey(String key, String value, int ttlSeconds) {
    Jedis jedis = redisResources.getJedis();
    jedis.set(key, value);
    if (ttlSeconds != 0) {
      jedis.expire(key,ttlSeconds);
    }
    redisResources.close(jedis);
  }


}
