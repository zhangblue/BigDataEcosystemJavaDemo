package zhangblue.hbase.test;

import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zhangblue.hbase.repository.HBaseResources;

public class TestClass {

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
  public void test1() throws Exception {

  }


}
