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
    String[] topics = {"bangcle_message_decrypt"};
    String groupId = "group_zhangdi_test8";
    examplesDemo.consumerDemo(kafkaBorkers, topics, groupId);
  }

  @Test
  public void testConsumer2() {
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String[] topics = {"bangcle_information"};
    String groupId = "group_test_02";
    examplesDemo.consumerDemo(kafkaBorkers, topics, groupId);
  }

  @Test
  public void testProducer() {
    String value="{\"server_time\":1522807391451,\"body\":{\"server_time\": \"1522823434202\",\"ip_lan\": \"172.16.19.138\",\"agent_id\": 44,\"start_id\": 1522823434,\"app_version\": \"unknown\",\"os_version\": \"unknown\",\"ip_src\": \"172.16.12.13\",\"user_data\": \"[{\\\"ccb_data\\\":[{\\\"phone\\\":\\\"14416533234\\\",\\\"id_card\\\":\\\"333220198909090000\\\",\\\"id\\\":1,\\\"bank_subbranch\\\":\\\"434343431\\\"}]}]\",\"version\": \"everisk 1.0 beta\",\"dt_server_time\": \"2018-04-04T14:30:34+08:00\",\"platform\": \"android\",\"dt_time\": \"2018-04-04T14:30:34+08:00\",\"manufacturer\": \"unknown\",\"protol_type\": \"userdata\",\"os_info\": \"android unknown\",\"extra\": \"{\\\"location\\\":{\\\"latitude\\\":\\\"31.2090626936\\\",\\\"longitude\\\":\\\"121.4934089939\\\"}}\",\"self_md5\": \"1999000000000000000000000000000000007\",\"protol_version\": 4,\"app_info\": \"android unknown\",\"time\": 1522823434195,\"udid\": \"4768-00000000-0000-0000-0000-000000004768\",\"msg_id\": 8561}}";
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String topics = "bangcle_message_decrypt";
    String key = "172.16.12.13";
    System.out.println(Math.floorMod(key.hashCode(), 8));
    //examplesDemo.producerDemo(kafkaBorkers, topics, key, value);
    for(int i=0;i<=7;i++){
      examplesDemo.producerDemo(kafkaBorkers, topics, i, key, value);
    }

  }

  @Test
  public void testProducer2() {
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String topics = "test_zhangd";
    String key = "test_key1";
    System.out.println(Math.floorMod(key.hashCode(), 2));
    examplesDemo.producerDemo(kafkaBorkers, topics, 4, key, "test_value");
  }

  @Test
  public void testConsumerToProducer() {
    String consumerBroker = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    String[] consumerTopic = {"bangcle_message_decrypt"};
    String consumerGroupid = "groupid_zhangd_001";
    String producerTopic = "bangcle_message_decrypt";
    String producerBokers = "172.16.36.148:9092,172.16.36.115:9092,172.16.36.147:9092";

    examplesDemo.consumerToProducer(consumerBroker, consumerTopic, consumerGroupid, producerTopic, producerBokers);
  }


}
