package cn.com.zhangblue.test;

import cn.com.zhangblue.demo.ExampleDemo;
import cn.com.zhangblue.repository.ElasticSearchRepository;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDemo {

  private ElasticSearchRepository elasticSearchRepository;


  @Before
  public void before() {
    elasticSearchRepository = new ElasticSearchRepository();
    try {
      elasticSearchRepository.initElasticsearchProperties();
      elasticSearchRepository.buildClient();
//      elasticSearchRepository.buildBulkProcessor();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @After
  public void after() {
    elasticSearchRepository.closeBulkProcessor();
    elasticSearchRepository.closeClient();
  }

  /**
   * 测试通过id查询数据
   */
  @Test
  public void getSourceByIdTest() {
    new ExampleDemo(elasticSearchRepository).getSourceById("index_zhangd_test", "zhangblue_type",
        "zhangblue");
  }


  /**
   * 测试es 批量入库
   */
  @Test
  public void testBulkFlush() {
    JSONObject jsonObject = new JSONObject().fluentPut("test", "333")
        .fluentPut("dt_server_time", System.currentTimeMillis());
    new ExampleDemo(elasticSearchRepository).addByBulk("index_zhangd_test", "zhangblue_type",
        "zhangblue", jsonObject);
  }

  /**
   * 测试es upsert
   */
  @Test
  public void testUpSert() {
    JSONObject jsonObjectIndex = new JSONObject().fluentPut("action", "index")
        .fluentPut("dt_server_time", System.currentTimeMillis());
    JSONObject jsonObjectUpdate = new JSONObject().fluentPut("action", "update1")
        .fluentPut("dt_server_time", System.currentTimeMillis());
    new ExampleDemo(elasticSearchRepository).addByUpsert("index_zhangd_test", "zhangblue_type",
        "zhangblue", jsonObjectIndex, jsonObjectUpdate);
  }
}
