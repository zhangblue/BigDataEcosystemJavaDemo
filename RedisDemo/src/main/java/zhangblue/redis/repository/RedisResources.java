package zhangblue.redis.repository;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Properties;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisResources {

  private Properties properties;
  protected JedisPool jedispool;


  /**
   * 读取配置文件
   */
  public void initJedisProperties() throws IOException {
    properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/config.properties"));
  }

  /**
   * 初始化redis链接池
   */
  public void initJedisPool() {
    try {
      JedisPoolConfig config = new JedisPoolConfig();
      // 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
      config.setBlockWhenExhausted(true);
      // 设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
      config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
      // 是否启用pool的jmx管理功能, 默认true
      config.setJmxEnabled(true);
      // 最大空闲连接数, 默认8个 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
      config.setMaxIdle(8);
      // 最大连接数, 默认8个
      config.setMaxTotal(200);
      // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
      config.setMaxWaitMillis(1000 * 100);
      // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
      config.setTestOnBorrow(true);

      if (Strings.isNullOrEmpty(properties.getProperty("redis.password"))) {
        jedispool = new JedisPool(config, properties.getProperty("redis.server.host"), Integer.parseInt(properties.getProperty("redis.server.port")), 3000);
      } else {
        jedispool = new JedisPool(config, properties.getProperty("redis.server.host"), Integer.parseInt(properties.getProperty("redis.server.port")), 3000,
            properties.getProperty("redis.password"));
      }
      System.out.println("redis init success");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  /**
   * 获取redis的连接.
   * 这块测试过了，在redis服务挂掉重启之后，可以自动建立连接，不用自己写代码来维护.
   */
  public Jedis getJedis() {
    try {
      if (jedispool != null) {
        Jedis resource = jedispool.getResource();
        return resource;
      } else {
        return null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * 释放jedis资源返回连接池
   */
  public void close(final Jedis jedis) {
    if (jedis != null) {
      try {
        jedis.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * 关闭redis连接池
   */
  public void closePool() {
    if (null != jedispool && !jedispool.isClosed()) {
      jedispool.close();
    }
  }
}
