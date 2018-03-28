package cn.com.zhangblue.demo;

import cn.com.zhangblue.repository.ElasticSearchRepository;
import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.unit.TimeValue;
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
  public void addByBulk(List<JSONObject> jsonSources) {

    for (JSONObject jsonSource : jsonSources) {
      IndexRequest indexRequest = new IndexRequest(jsonSource.getString("index"), jsonSource.getString("type"),
          jsonSource.getString("id")).source(jsonSource.getJSONObject("source"));
      elasticSearchRepository.getBulkProcessor().add(indexRequest);
    }
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
    String status = updateResponse.getResult().getLowercase();
    int status_id = updateResponse.status().getStatus();

    if (status.equals("updated") || status_id == 200) {
      System.out.println("update message success");
    } else if (status.equals("created") || status_id == 201) {
      System.out.println("create message success");
    }
    System.out.println(updateResponse.getResult().getLowercase() + "=====" + updateResponse.status() + "=====" + updateResponse.status().getStatus());
  }

  /**
   * 得到指定index的状态
   *
   * @param indices index列表
   */
  public void indexStat(String[] indices) {
    ClusterAdminClient clusterAdminClient = elasticSearchRepository.getTransportClient().admin().cluster();
    ClusterHealthResponse clusterHealthResponse = clusterAdminClient.health(new ClusterHealthRequest(indices)).actionGet();
    Map<String, ClusterIndexHealth> indexHealthMap = clusterHealthResponse.getIndices();
    Iterator<String> itIndices = indexHealthMap.keySet().iterator();
    while (itIndices.hasNext()) {
      String key = itIndices.next();
      ClusterIndexHealth clusterIndexHealth = indexHealthMap.get(key);
      System.out.println(clusterIndexHealth.getStatus() + "---" + clusterIndexHealth.getIndex() + "---" + clusterIndexHealth
          .getActivePrimaryShards() + "---" + clusterIndexHealth.getNumberOfReplicas());
    }
  }

  /**
   * 删除指定index的数据
   *
   * @param index index名
   * @param type type
   * @param id id
   */
  public void deleteById(String index, String type, String id) {
    DeleteResponse response = elasticSearchRepository.getTransportClient().prepareDelete(index, type, id).get(TimeValue.timeValueSeconds(1));
    String result = response.getResult().getLowercase();
    int status_id = response.status().getStatus();

    if (status_id == 200 || result.equals("deleted")) {
      System.out.println("delete success");
    } else if (status_id == 404 || result.equals("not_found")) {
      System.out.println("not found");
    } else {
      System.out.println(result);
    }
  }

  /**
   * 插入单挑数据
   *
   * @param index index名
   * @param type type
   * @param id id
   * @param jsonSource 数据体
   */
  public void insertSource(String index, String type, String id, JSONObject jsonSource) {
    IndexResponse indexResponse = elasticSearchRepository.getTransportClient().prepareIndex(index, type, id).setSource(jsonSource).get(TimeValue.timeValueSeconds(1));

    System.out.println(indexResponse.toString());
  }


}
