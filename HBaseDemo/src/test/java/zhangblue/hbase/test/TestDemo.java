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
  public void testCreateHbaseTable() {
    try {
      new ExampleDemo(hBaseResources)
          .createHbaseTable("test_zhangd", "info1", 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertHbase() {
    String str = "bbb";
    try {
      new ExampleDemo(hBaseResources)
          .putToHbase("test_zhangd", "family1", "info2", "testone", str);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testListTables() {
    try {
      new ExampleDemo(hBaseResources).listTables();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testTableAddFamily() {
    try {
      new ExampleDemo(hBaseResources).tableAddFamily("test_zhangd", "family2");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDeleteByRowKey() {
    try {
      new ExampleDemo(hBaseResources).deleteByRowKey("test_zhangd", "testone");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCleanFamily() {
    try {
      new ExampleDemo(hBaseResources).cleanFamily("test_zhangd", "testone", "family1");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDeleteFamily(){
    try {
      new ExampleDemo(hBaseResources).deleteFamily("test_zhangd","info2");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testColumnList(){
    try {
      new ExampleDemo(hBaseResources).columnList("test_zhangd","family1","testone");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
