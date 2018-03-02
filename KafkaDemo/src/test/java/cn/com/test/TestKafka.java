package cn.com.test;

import cn.com.kafka.demo.Examples;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestKafka {
  private Examples examples;
  @Before
  public void before(){
    examples = new Examples();
  }

  @After
  public void after(){
  }

  @Test
  public void testConsumer(){
    examples.consumerDemo();
  }
}
