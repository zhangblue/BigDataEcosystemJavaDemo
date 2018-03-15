package cn.com.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zhangblue.kafka.demo.ExamplesDemo;

public class TestKafka {

  private ExamplesDemo examplesDemo;

  @Before
  public void before() {
    examplesDemo = new ExamplesDemo();
  }

  @After
  public void after() {
  }

  @Test
  public void testConsumer() {
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String[] topics = {"test_zhangd"};
    String groupId = "group_test_01";
    examplesDemo.consumerDemo(kafkaBorkers, topics, groupId);
  }

  @Test
  public void testProducer() {
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String topics = "test_zhangd";
    String key = "test_key1";
    System.out.println(Math.floorMod(key.hashCode(),2));
    examplesDemo.producerDemo(kafkaBorkers, topics, key, "test_value");
  }

  @Test
  public void testProducer2() {
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String topics = "test_zhangd";
    String key = "test_key1";
    System.out.println(Math.floorMod(key.hashCode(),2));
    examplesDemo.producerDemo(kafkaBorkers, topics,4, key, "test_value");
  }
}
