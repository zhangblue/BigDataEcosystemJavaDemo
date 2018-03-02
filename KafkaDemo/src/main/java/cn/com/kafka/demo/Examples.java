package cn.com.kafka.demo;

import cn.com.kafka.connection.KafkaConnect;
import cn.com.kafka.connection.KafkaOffsetResetEnum;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Examples {

  /***
   * 消费者Demo
   */
  public void consumerDemo() {
    KafkaConnect kafkaConnect = new KafkaConnect();
    String kafkaBorkers = "172.16.31.66:9092,172.16.31.75:9092,172.16.31.80:9092";
    Properties properties = kafkaConnect
        .getPropsConsumer("group_test_01", kafkaBorkers, KafkaOffsetResetEnum.EARLIEST);

    KafkaConsumer consumer = new KafkaConsumer(properties);
    consumer.subscribe(Arrays.asList("topic_threat"));

    File file = new File("/Users/zhangdi/test_folder/data_test/message");
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      System.out.println("poll count =" + records.count());

      for (ConsumerRecord<String, String> record : records) {
        String kafkaKey = record.key();
        String kafkaValue = record.value();
        try {
          Files.append(kafkaKey + "=====" + kafkaValue + "\n", file, Charset.defaultCharset());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
