package zhangblue.hbase.test;

import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zhangblue.hbase.demo.ExampleDemo;
import zhangblue.hbase.repository.HBaseResources;

public class TestDemo {


  private HBaseResources hBaseResources;

  @Before
  public void before() {
    hBaseResources = new HBaseResources();
    hBaseResources.initHBaseConfig();
    try {
      hBaseResources.initConnection();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @After
  public void after() {
    try {
      hBaseResources.closeConnection();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetByRowKey() {
    new ExampleDemo(hBaseResources).getByRowKey("bangcle_config_sync", "info", "cheat_host_rule");
  }

  @Test
  public void testCreateHbaseTable() {
    new ExampleDemo(hBaseResources)
        .createHbaseTable("bangcle_safe_event_threat_list", "threat_list", 3600);
  }

  @Test
  public void testInsertHbase() {
    String str = "[{\"threat_time\":\"1520748093046\",\"threat_id\":\"leo13888-5555-7777-8888-bangcle01111_371_4_e_speed_1520748075269_17052\",\"threat_type\":\"e_speed\"}]";
    new ExampleDemo(hBaseResources)
        .putToHbase("bangcle_safe_event_threat_list", "threat_list", "info", "testone", str);
  }
}
