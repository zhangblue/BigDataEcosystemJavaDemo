package zhangblue.kafka.demo;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
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
        .getPropsConsumer(groupId, borkers, KafkaOffsetResetEnum.LAST);
    KafkaConsumer consumer = new KafkaConsumer(properties);
    consumer.subscribe(Arrays.asList(topic));
    File file = new File("/Users/zhangdi/test_folder/data_test/message_crash");
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      System.out.println(records.count());
      for (ConsumerRecord<String, String> record : records) {
        String line =
            "partition=[{" + record.partition() + "}] , topic=[{" + record.topic() + "}] , offset=[{" + record.offset() + "}] , key=[{" + record.key() + "}] , value=[{" + record
                .value() + "}]";
        JSONObject jsonObject = JSONObject.parseObject(record.key());
        if (jsonObject.getString("message_type").equals("crash")) {
          JSONObject jsonValue = JSONObject.parseObject(record.value());

          if (jsonValue.containsKey("data") && jsonValue.get("data") instanceof JSONObject) {

          }
        }
        //System.out.println(line);
      }
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
}
