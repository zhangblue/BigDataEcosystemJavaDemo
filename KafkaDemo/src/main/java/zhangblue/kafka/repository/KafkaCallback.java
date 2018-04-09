package zhangblue.kafka.repository;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka producer回调函数
 */
public class KafkaCallback implements Callback {

  private String key;
  private String value;

  public KafkaCallback(String key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//    if (null == e) {
//      Log4j2Factory.logger
//          .info("success! topic=[{}] , partition=[{}] , offset=[{}] , key=[{}] , value=[{}]", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), key,
//              value);
//    } else {
//      Log4j2Factory.logger
//          .info("error! topic=[{}] , partition=[{}] , offset=[{}] , key=[{}] , value=[{}]", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), key,
//              value);
//      // Log4j2Factory.logger.error("error", e);
//    }
  }
}
