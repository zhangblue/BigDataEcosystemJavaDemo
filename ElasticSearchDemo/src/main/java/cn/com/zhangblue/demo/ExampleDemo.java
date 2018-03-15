package cn.com.zhangblue.demo;

import cn.com.zhangblue.repository.ElasticSearchRepository;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;

public class ExampleDemo {

  private ElasticSearchRepository elasticSearchRepository;

  public ExampleDemo(ElasticSearchRepository elasticSearchRepository) {
    this.elasticSearchRepository = elasticSearchRepository;
  }

  /**
   * 测试通过_id查询数据
   */
  public void getSourceById(String index, String type, String id) {
    TransportClient client = elasticSearchRepository.getTransportClient();

    GetRequestBuilder getRequestBuilder = client
        .prepareGet(index, type,
            id);
    GetResponse getResponse = getRequestBuilder.get();
    if (getResponse.isExists()) {
      System.out.println(JSONObject.toJSONString(getResponse.getSourceAsMap()));
    } else {
      System.out.println("not exist");
    }
  }

  public void getQuery(String[] indexs, String keyName1, String keyValue1, String keyName2, String keyValue2) {
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    TermsQueryBuilder queryBuilder1 = QueryBuilders
        .termsQuery(keyName1, keyValue1);
    TermsQueryBuilder queryBuilder2 = QueryBuilders
        .termsQuery(keyName2, keyValue2);
    boolQueryBuilder.must(queryBuilder1);
    boolQueryBuilder.must(queryBuilder2);

    SearchRequestBuilder searchRequestBuilder = elasticSearchRepository.getTransportClient().prepareSearch(indexs).setTypes("").setQuery(boolQueryBuilder).setSize(100);

  }

  /**
   * 批量入库
   */
  public void addByBulk(String index, String type, String id, JSONObject jsonObject) {
    IndexRequest indexRequest = new IndexRequest(index, type,
        id).source(jsonObject);
    elasticSearchRepository.getBulkProcessor().add(indexRequest);
    elasticSearchRepository.getBulkProcessor().flush();//flush是立刻提交bulk中的操作。否则需要等待bulk周期自行刷新
  }

  /**
   * upsert入库
   */
  public void addByUpsert(String index, String type, String id, JSONObject insertJson,
      JSONObject updateJson) {

    IndexRequest indexRequest = new IndexRequest(index, type, id).source(insertJson);
    UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(updateJson)
        .upsert(indexRequest);

    UpdateResponse updateResponse = elasticSearchRepository.getTransportClient()
        .update(updateRequest).actionGet();
  }

  public void renameIndex(String oldName, String newName) {
   AdminClient admin =  elasticSearchRepository.getTransportClient().admin();

   IndicesAdminClient indicesAdminClient = admin.indices();
  }
}
