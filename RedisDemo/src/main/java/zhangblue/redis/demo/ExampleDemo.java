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
   * @param key
   * @param value
   */
  public void setKey(String key, String value) {
    Jedis jedis = redisResources.getJedis();
    jedis.set(key, value);
    redisResources.close(jedis);
  }



}
