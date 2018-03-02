package cn.com.kafka.connection;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * kafka 初始化类
 */
public class KafkaConnect {

  /***
   * 初始化kafka consumer配置
   * @param strGroupId groupid
   * @param kafkaBrokderAddrs kafka broker地址列表
   * @param kafkaOffsetResetEnum 读取offset时的读取方式
   * @return
   */
  public Properties getPropsConsumer(String strGroupId, String kafkaBrokderAddrs,
      KafkaOffsetResetEnum kafkaOffsetResetEnum) {
    Properties propsConsumer = new Properties();
    propsConsumer
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokderAddrs);
    propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, strGroupId);
    propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    propsConsumer.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    propsConsumer.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        kafkaOffsetResetEnum.getAction());//latest, earliest, none

    return propsConsumer;
  }

  /**
   * 初始化producer的Properties信息
   */
  public Properties getPropsProducer(String kafkaBrokderAddrs) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", kafkaBrokderAddrs);
    properties.put("metadata.broker.list", kafkaBrokderAddrs);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("request.required.acks", "1");
    return properties;
  }

}
