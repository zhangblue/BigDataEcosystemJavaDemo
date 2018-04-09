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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.unit.TimeValue;
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
    new ExampleDemo(elasticSearchRepository).addByUpsert("bangcle_zhangd", "zhangblue_type",
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

    String line = "{\"agent_id\":372,\"start_id\":1522652728478,\"msg_id\":1,\"udid\":\"7c207892-1019-4aa2-991f-c43e1fc4047d\",\"self_md5\":\"9hzktrg6bkmst9pchot6ffb2e6o=\",\"version\":\"everisk iOS4.0.0-0111\",\"protol_version\":1,\"protol_type\":\"devinfo\",\"time\":1522652728529,\"platform\":\"ios\",\"extra\":null,\"server_time\":1522652728614,\"client_ip\":\"172.16.18.22\",\"location\":\"\",\"run_key\":\"883f822b478aa313845e280f12fff0e8\",\"ip_lan\":\"172.16.18.22\",\"ip_wan\":null,\"host\":\"iPhone7(is_new)\",\"model\":\"iPhone 7\",\"is_root\":null,\"os_name\":\"iOS\",\"os_version\":\"10.2.1\",\"resolution_w\":\"750\",\"resolution_h\":\"1334\",\"networking_mode\":\"WIFI\",\"carrier\":\"中国联通\",\"mcc\":null,\"mnc\":null,\"iso\":null,\"allows_voip\":true,\"battery_level\":\"1.00\",\"proximity_state\":false,\"multitasking_supported\":true,\"cpu_abi\":\"arm64\",\"manufacturer\":\"Apple\",\"app_name\":\"EveriskTest\",\"app_version\":\"1.0\",\"net_type\":\"\",\"os_info\":\"ios 10.2.1\",\"app_info\":\"ios 1.0\",\"data_type\":\"devinfo\"}";

    JSONObject jsonSource = JSONObject.parseObject(line);

    String udid = jsonSource.getString("udid");

    String agent_id = jsonSource.getString("agent_id");
    fixDate(jsonSource, "time");
    fixDate(jsonSource, "server_time"); //处理json数据
    //必须存在的字段
    //可有可无的
    fixObj(jsonSource, "extra");

    //dev获取id的方式和其它逻辑不一样, by zhaogj
    String strId = udid + "|" + agent_id;

    boolean f = checkDevinfoChange(strId, jsonSource);
    System.out.println(strId);
    System.out.println(f);


    if (f) {
      new ExampleDemo(elasticSearchRepository).insertSource("bangcle_devinfo", "bangcle_type", strId, jsonSource);
    } else {
      System.out.println("1111111");
    }
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

  /***
   * 判断发来的消息除server_time以外其他字段在es中是否都没有改变过，如果没有改变，则不需要进行upsert操作
   * @param jsonObject
   * @return 返回是否需要更新设备信息, true为需要添加，false为不需要添加
   */
  private boolean checkDevinfoChange(String strid, JSONObject jsonObject) {
    boolean bflage = false;
    try {
      GetRequestBuilder getRequest = elasticSearchRepository.getTransportClient()
          .prepareGet("bangcle_devinfo", "bangcle_type", strid);

      GetResponse getResponse = getRequest.get(TimeValue.timeValueSeconds(2));

      if (getResponse.isExists()) {
        Map<String, Object> kafkaMap = JSONObject.parseObject(jsonObject.toJSONString(), Map.class);
        //删除变化的字段
        if (kafkaMap.containsKey("start_id")) {
          kafkaMap.remove("start_id");
        }
        if (kafkaMap.containsKey("msg_id")) {
          kafkaMap.remove("msg_id");
        }
        if (kafkaMap.containsKey("time")) {
          kafkaMap.remove("time");
        }
        if (kafkaMap.containsKey("dt_time")) {
          kafkaMap.remove("dt_time");
        }
        if (kafkaMap.containsKey("server_time")) {
          kafkaMap.remove("server_time");
        }
        if (kafkaMap.containsKey("dt_server_time")) {
          kafkaMap.remove("dt_server_time");
        }
        if (kafkaMap.containsKey("run_key")) {
          kafkaMap.remove("run_key");
        }

        Map<String, Object> mapEs = getResponse.getSourceAsMap();
        Iterator<String> iteratorKey = kafkaMap.keySet().iterator();
        while (iteratorKey.hasNext()) {
          String messageKey = iteratorKey.next();

          if (mapEs.containsKey(messageKey)) {
            if (kafkaMap.get(messageKey) == null) {
              //如果kafka数据是null，直接跳过
              continue;
            } else if (mapEs.get(messageKey) == null) {
              //如果kafka数据不是null，但是es里是空的，则需要进行更新
              bflage = true;
              break;
            } else if (mapEs.get(messageKey).toString()
                .equals(kafkaMap.get(messageKey).toString())) {
            } else {
              bflage = true;
              break;
            }
          } else {
            bflage = true;
            break;
          }
        }
      } else {
        bflage = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
      bflage = true;
    }
    return bflage;
  }
}
