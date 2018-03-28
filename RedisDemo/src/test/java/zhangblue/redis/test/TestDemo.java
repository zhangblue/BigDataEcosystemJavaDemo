package zhangblue.redis.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zhangblue.redis.demo.ExampleDemo;
import zhangblue.redis.repository.RedisResources;

public class TestDemo {

  private RedisResources redisResources;

  @Before
  public void before() {
    redisResources = new RedisResources();
    redisResources.initJedisPool();
  }

  @After
  public void after() {
    redisResources.closePool();
  }

  @Test
  public void testSetKey() {
    new ExampleDemo(redisResources).setKey("a", "1");
  }

}
