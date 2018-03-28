package cn.com.zhangblue.test;

import cn.com.zhangblue.repository.ElasticSearchRepository;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAll {

  private ElasticSearchRepository elasticSearchRepository;

  @Before
  public void before() {
    elasticSearchRepository = new ElasticSearchRepository();
    try {
      elasticSearchRepository.initElasticsearchProperties();
      elasticSearchRepository.buildClient();
      elasticSearchRepository.buildBulkProcessor();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @After
  public void after() {
    elasticSearchRepository.closeBulkProcessor();
    elasticSearchRepository.closeClient();
  }

  @Test
  public void test1() {

  }
}
