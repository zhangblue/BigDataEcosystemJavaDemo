package zhangblue.kafka.repository;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * kafka 初始化类
 */
public class KafkaRepository {

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
    propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        kafkaOffsetResetEnum.getAction());//latest, earliest, none
    propsConsumer.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2097152);
    return propsConsumer;
  }

  /**
   * 初始化producer的Properties信息
   */
  public Properties getPropsProducer(String kafkaBrokderAddrs) {
    Properties properties = new Properties();

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokderAddrs);
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 2097152);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.ACKS_CONFIG, "1");
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);//设置不重发
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//表示数据积攒多少后发送一次
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);//请求积攒5毫秒发送一次

//    properties.put("serializer.class", "kafka.serializer.StringEncoder");
//    properties.put("metadata.broker.list", kafkaBrokderAddrs);

    return properties;
  }

}
