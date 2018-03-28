package cn.com.zhangblue.repository;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchRepository {

  private Properties properties;
  private TransportClient transportClient;
  private BulkProcessor bulkProcessor;


  public void initElasticsearchProperties() throws IOException {
    properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/config.properties"));
  }

  /**
   * 创建 elasticsearch client
   *
   * 设置链接集群的名字 cluster.name
   * 设置嗅探 false
   **/
  public void buildClient() throws UnknownHostException {
    Settings settings = Settings.builder()
        .put("cluster.name", properties.getProperty("elasticsearch.clouster.name"))
        .put("client.transport.sniff", true).build();
//    Iterable<String> iterableHostName = Splitter.on(",").trimResults()
//        .split(properties.getProperty("elasticsearch.address"));
    String[] iterableHostName = {properties.getProperty("elasticsearch.address")};
    transportClient = new PreBuiltTransportClient(settings);

    for (String strTransportHostName : iterableHostName) {
      transportClient.addTransportAddress(
          new TransportAddress(InetAddress.getByName(strTransportHostName), Integer.parseInt(properties.getProperty("elasticsearch.port"))));
    }
  }


  /***
   * 初始化批量入库对象
   */
  public void buildBulkProcessor() {
    bulkProcessor = BulkProcessor.builder(transportClient, new Listener() {
      //bulk提交前执行
      @Override
      public void beforeBulk(long l, BulkRequest bulkRequest) {
        System.out.println("commit action count = " + bulkRequest.numberOfActions());
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
          System.out.println("has fails");
        } else {
          System.out.println("no fails");
        }
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
        System.out.println(bulkRequest.getDescription());
        System.out.println(throwable.getStackTrace());
      }
    }).setBulkActions(1000).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)).setFlushInterval(
        TimeValue.timeValueSeconds(10)).setConcurrentRequests(3)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();
  }

  public void closeClient() {
    if (transportClient != null) {
      transportClient.close();
    }
  }

  public void closeBulkProcessor() {
    if (bulkProcessor != null) {
      bulkProcessor.close();
    }
  }

  /**
   * delete template.
   */
  public void deleteTemplate(String strTemplateName) {
    transportClient.admin().indices().prepareDeleteTemplate(strTemplateName).get();
  }

  /**
   * put template.
   */
  public void putTemplate() {
    try {
      Map<String, Object> settings = new MapBuilder<String, Object>()
          .put("number_of_shards", 1)
          .put("number_of_replicas", 0)
          .put("refresh_interval", "10s")
          .map();
      Map<String, Object> mapping = new HashMap<String, Object>();
      mapping.put("_all", new MapBuilder<String, Object>().put("enabled", false).map());
      mapping.put("numeric_detection", false);
      mapping.put("dynamic_templates",
          new Object[]{
              new MapBuilder<String, Object>().put("date_tpl",
                  new MapBuilder<String, Object>().put("match", "dt*")
                      .put("mapping",
                          new MapBuilder<String, Object>().put("type", "date")
                              .map())
                      .map())
                  .map(),
              new MapBuilder<String, Object>().put("geo_point_tpl",
                  new MapBuilder<String, Object>().put("match", "geop*")
                      .put("mapping",
                          new MapBuilder<String, Object>().put("type", "geo_point")
                              .map())
                      .map())
                  .map(),
              new MapBuilder<String, Object>().put("ip_tpl",
                  new MapBuilder<String, Object>().put("match", "ip*")
                      .put("mapping",
                          new MapBuilder<String, Object>().put("type", "ip")
                              .map())
                      .map())
                  .map(),
              new MapBuilder<String, Object>().put("obj_tpl",
                  new MapBuilder<String, Object>().put("match", "obj*")
                      .put("mapping",
                          new MapBuilder<String, Object>().put("type", "object")
                              .map())
                      .map())
                  .map(),
              new MapBuilder<String, Object>().put("all_tpl",
                  new MapBuilder<String, Object>().put("match", "*").put("mapping",
                      new MapBuilder<String, Object>().put("type", "keyword")
                          .map())
                      .map())
                  .map()});
      transportClient.admin().indices().preparePutTemplate("template_bangcle")
          .setPatterns(Collections.singletonList("index_*"))
          .setSettings(settings)
          .setOrder(0)
          .addMapping("_default_", mapping)
          .get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * check index exist.
   */
  public boolean exists(String strIndex) {
    IndicesExistsRequest request = new IndicesExistsRequest(strIndex);
    IndicesExistsResponse response = transportClient.admin().indices().exists(request).actionGet();
    if (response.isExists()) {
      return true;
    }
    return false;
  }

  /**
   * delete index.
   */
  public void delete(String strIndex) {
    if (exists(strIndex)) {
      transportClient.admin().indices().prepareDelete(strIndex).get();
    }
  }

  /**
   * create index.
   */
  public void create(String strIndex, int numShards, int numReplicas) {
    transportClient.admin().indices().prepareCreate(strIndex)
        .setSettings(Settings.builder()
            .put("index.number_of_shards", numShards)
            .put("index.number_of_replicas", numReplicas)
            .put("index.refresh_interval", "10s")
        ).get();
  }

  public Properties getProperties() {
    return properties;
  }

  public TransportClient getTransportClient() {
    return transportClient;
  }

  public BulkProcessor getBulkProcessor() {
    return bulkProcessor;
  }
}
