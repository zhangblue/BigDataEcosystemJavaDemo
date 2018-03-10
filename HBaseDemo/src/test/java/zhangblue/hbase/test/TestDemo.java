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

}
