package cn.com.zhangblue.test;

import cn.com.zhangblue.demo.ExampleDemo;
import cn.com.zhangblue.repository.ElasticSearchRepository;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
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
  public void testAddBulkFlush() {
    List<JSONObject> jsonObjectList = new ArrayList<>();

    JSONObject jsonObject1 = new JSONObject();
    JSONObject jsonSource1 = new JSONObject();
    jsonSource1.put("action", "this is test1");
    jsonObject1.fluentPut("index", "index_zhangd_test").fluentPut("type", "zhangblue_type").fluentPut("id", "zhangblue1").fluentPut("source", jsonSource1);
    jsonObjectList.add(jsonObject1);

    JSONObject jsonObject2 = new JSONObject();
    JSONObject jsonSource2 = new JSONObject();
    jsonSource2.put("action", "this is test1");
    jsonObject2.fluentPut("index", "index_zhangd_test").fluentPut("type", "zhangblue_type").fluentPut("id", "zhangblue2").fluentPut("source", jsonSource2);
    jsonObjectList.add(jsonObject2);

    new ExampleDemo(elasticSearchRepository).addByBulk(jsonObjectList);
  }

  /**
   * 测试es upsert
   */
  @Test
  public void testAddUpSert() {
    JSONObject jsonObjectIndex = new JSONObject().fluentPut("action", "index")
        .fluentPut("dt_server_time", System.currentTimeMillis());
    JSONObject jsonObjectUpdate = new JSONObject().fluentPut("action", "update1")
        .fluentPut("dt_server_time", System.currentTimeMillis());
    new ExampleDemo(elasticSearchRepository).addByUpsert("index_zhangd_test", "zhangblue_type",
        "zhangblue", jsonObjectIndex, jsonObjectUpdate);
  }

  @Test
  public void test() {
    ZonedDateTime z = ZonedDateTime.ofInstant(Instant.ofEpochSecond(1522119000539L), ZoneId.of("+08:00"));
    System.out.println(z.toLocalDate());
  }

  /**
   * 删除
   */
  @Test
  public void testDeleteById() {
    new ExampleDemo(elasticSearchRepository).deleteById("index_zhangd_test", "zhangblue_type", "zhangblue");
  }

  /**
   * 测试插入单条数据
   */
  @Test
  public void testInsertSource() {

    String line = "{\"ip_lan\":\"172.16.19.138\",\"agent_id\":1,\"start_id\":1522202199,\"user_data\":[{\"ccb_data\":[{\"phone\":\"13412120990\",\"id_card\":\"999220198909091212\",\"id\":1,\"bank_subbranch\":\"中国建设银行北七家支行\"}]}],\"version\":\"everisk 1.0 beta\",\"platform\":\"android\",\"protol_type\":\"userdata\",\"extra\":{\"location\":{\"latitude\":\"39.8822627832\",\"longitude\":\"116.5494035971\"}},\"self_md5\":\"1000000000000000000000000000000005\",\"protol_version\":4,\"time\":1522202199642,\"udid\":\"4070-00000000-0000-0000-0000-000000004070\",\"msg_id\":1397,\"run_key\":\"93dd83e8fc510672f1d2931bca2cde4d\",\"server_time\":1522202199598,\"client_ip\":\"172.16.12.13\",\"redis_ip\":\"\",\"model\":\"Redmi 4A5\",\"os_version\":\"6.0.15\",\"manufacturer\":\"Xiaomi5\",\"imei\":[864701034210003],\"app_name\":\"测试应用\",\"app_version\":\"5.0.0\",\"net_type\":\"NETWORK_WIFI\",\"location\":null,\"os_info\":\"android 6.0.15\",\"app_info\":\"android 5.0.0\",\"data_type\":\"userdata\"}";

    JSONObject jsonSource = JSONObject.parseObject(line);

    //得到index
    long lServerTime = Long.parseLong(jsonSource.getString("server_time"));
    String strDate = getStringByLong(lServerTime / 1000L, "yyyyMMdd");
    String strIndex = "bangcle_user_data_" + strDate;
    //得到id runkey|agent_id
    String id = jsonSource.getString("run_key") + "|" + jsonSource.getString("agent_id");

    //整理数据
    fixDate(jsonSource, "time");
    fixDate(jsonSource, "server_time");

    fixObj(jsonSource, "extra");
    fixObj(jsonSource, "user_data");

    new ExampleDemo(elasticSearchRepository).insertSource(strIndex, "bangcle_type", id, jsonSource);
  }

  /**
   * 将obj字段强转成string by zhaogj
   */
  public void fixObj(Object obj, String strKey) {
    //转成字符型，后续有需要在用obj
    if (obj instanceof JSONObject) {
      JSONObject json = (JSONObject) obj;
      Object objValue = json.get(strKey);
      if (objValue != null) {
        json.put(strKey, objValue.toString());
      }
    }
  }

  /**
   * 将json中的指定key修改字段名称，修改成功返回true, by zhaogj
   */
  public boolean fixDate(JSONObject json, String strKey) {
    if (json.containsKey(strKey)) {
      long lTime = json.getLong(strKey);
      json.put("dt_" + strKey,
          getEsDateByLong(lTime / 1000L));
      return true;
    } else {
      return false;
    }
  }

  /**
   * 将绝对毫秒数转成ES接受的时区时间格式,时区东八区.
   */
  public ZonedDateTime getEsDateByLong(long longTime) {

    return ZonedDateTime.ofInstant(Instant.ofEpochSecond(longTime), ZoneId.of("+08:00"));
  }

  public String getStringByLong(long longTime, String strFormatter) {
    Instant instant = Instant.ofEpochSecond(longTime);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("+08:00"));
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(strFormatter);
    return localDateTime.format(dateTimeFormatter);
  }
}
