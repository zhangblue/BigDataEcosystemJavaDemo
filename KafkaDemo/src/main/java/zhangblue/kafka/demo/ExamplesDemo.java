package zhangblue.kafka.demo;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import zhangblue.kafka.repository.KafkaCallback;
import zhangblue.kafka.repository.KafkaOffsetResetEnum;
import zhangblue.kafka.repository.KafkaRepository;

public class ExamplesDemo {

  /***
   * 消费者Demo
   */
  public void consumerDemo(String borkers, String[] topic, String groupId) {
    KafkaRepository kafkaRepository = new KafkaRepository();
    Properties properties = kafkaRepository
        .getPropsConsumer(groupId, borkers, KafkaOffsetResetEnum.EARLIEST);
    KafkaConsumer consumer = new KafkaConsumer(properties);
    consumer.subscribe(Arrays.asList(topic));

    File file = new File("/Users/zhangdi/test_folder/data_test/data_kafka_zhangdi_test6");
    File file2 = new File("/Users/zhangdi/test_folder/data_test/data_kafka_zhangdi_test6_all");
    File file3 = new File("/Users/zhangdi/test_folder/data_test/data_kafka_zhangdi_error");

    file3.delete();
    int count = 0;

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      System.out.println("records = " + records.count());
      for (ConsumerRecord<String, String> record : records) {
        String line = record.value();
        JSONObject jsonObject = JSONObject.parseObject(line);
        long servertime = jsonObject.getLongValue("server_time");
        JSONObject jsonBody = jsonObject.getJSONObject("body");
        if (!Strings.isNullOrEmpty(jsonBody.getString("protol_type"))) {
          if (jsonBody.getString("protol_type").equals("userdata")) {
            count++;
            try {
              Files
                  .append(jsonBody.getString("protol_type") + "\t" + jsonBody.getString("udid") + "\t" + servertime + "\t" + record.partition() + "\t" + record.offset() + "\n",
                      file,
                      Charset.defaultCharset());
              Files
                  .append(record.value() + "\n", file2, Charset.defaultCharset());
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        } else {
          try {
            Files
                .append(record.value() + "\n", file3, Charset.defaultCharset());
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
      System.out.println("now = " + count);
    }
  }

  /**
   * 使用默认分区方式 生产者demo
   *
   * @param borkers kafka brokers地址
   * @param topic topic名字
   * @param key key
   * @param value value
   */
  public void producerDemo(String borkers, String topic, String key, String value) {
    KafkaRepository kafkaRepository = new KafkaRepository();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaRepository.getPropsProducer(borkers));
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    kafkaProducer.send(record, new KafkaCallback(key, value));
    kafkaProducer.flush();
  }

  /**
   * 指定paration 生产者
   *
   * @param borkers kafka brokers地址
   * @param topic topic名字
   * @param paration paration编号
   * @param key key
   * @param value value
   */
  public void producerDemo(String borkers, String topic, int paration, String key, String value) {
    KafkaRepository kafkaRepository = new KafkaRepository();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaRepository.getPropsProducer(borkers));
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, paration, key, value);
    kafkaProducer.send(record, new KafkaCallback(key, value));
    kafkaProducer.flush();
  }

  /***
   * 消费者Demo
   */
  public void consumerToProducer(String consumerBorkers, String[] consumerTopic, String groupId, String producerTopic, String producerBorkers) {
    KafkaRepository kafkaRepository = new KafkaRepository();
    Properties propertiesConsumer = kafkaRepository
        .getPropsConsumer(groupId, consumerBorkers, KafkaOffsetResetEnum.LAST);

    Properties propertiesproducer = kafkaRepository.getPropsProducer(producerBorkers);

    KafkaConsumer consumer = new KafkaConsumer(propertiesConsumer);
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(propertiesproducer);

    consumer.subscribe(Arrays.asList(consumerTopic));

    File file = new File("/Users/zhangdi/test_folder/data_test/data_kafka_zhangdi_001");

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      System.out.println("consumer count = " + records.count());
      for (ConsumerRecord<String, String> record : records) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(producerTopic, record.key(), record.value());
        kafkaProducer.send(producerRecord, new KafkaCallback(record.key(), record.value()));
//        try {
//          Files.append(record.value() + "\t" + record.partition() + "\t" + record.offset() + "\n", file, Charset.defaultCharset());
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
      }
    }
  }
}
