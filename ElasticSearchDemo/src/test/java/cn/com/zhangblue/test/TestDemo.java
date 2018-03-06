package cn.com.zhangblue.test;

import cn.com.zhangblue.repository.ElasticSearchRepository;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
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
  public void testGetOne() {
    TransportClient client = elasticSearchRepository.getTransportClient();

    GetRequestBuilder getRequestBuilder = client
        .prepareGet("index_message_20180306", "bangcle_type",
            "172.16.11.11_1520319321700_4d60628f-65ad-3806-ae21-e48f4044e147_102_20");

    GetResponse getResponse = getRequestBuilder.get();

    if (getResponse.isExists()) {
      System.out.println(getResponse.getSourceAsMap().get("gyroscope"));
    } else {
      System.out.println("error");
    }
  }


  @Test
  public void testBulkFlush() {
    JSONObject jsonObject = new JSONObject().fluentPut("test", "333").fluentPut("dt_server_time",System.currentTimeMillis());
    IndexRequest indexRequest = new IndexRequest("index_message_20180306", "bangcle_type",
        "test_zd").source(jsonObject);
    elasticSearchRepository.getBulkProcessor().add(indexRequest);
    elasticSearchRepository.getBulkProcessor().flush();
    System.out.println("add time ="+System.currentTimeMillis());
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
