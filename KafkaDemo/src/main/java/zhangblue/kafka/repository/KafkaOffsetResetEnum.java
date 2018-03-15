package zhangblue.kafka.repository;

public enum KafkaOffsetResetEnum {

  /***
   * latest: 从最新的offset开始读，之前没读的都不要了
   * earliest: 从现在最小的offset开始读
   */
  LAST("latest"), EARLIEST("earliest"), NONE("none");
  private final String action;

  KafkaOffsetResetEnum(String action) {
    this.action = action;
  }

  public String getAction() {
    return action;
  }
}
