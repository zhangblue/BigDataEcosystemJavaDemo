package cn.com.java.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.Test;

public class TestJava {

  @Test
  public void test01() {
    Map<String, String> map = new TreeMap<>(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    });

    map.put("8", "a");
    map.put("9", "b");
    map.put("1", "c");
    map.put("3", "d");
    map.put("7", "e");

    Set<String> keySet = map.keySet();
    Iterator<String> iter = keySet.iterator();
    while (iter.hasNext()) {
      String key = iter.next();
      System.out.println(key + ":" + map.get(key));
    }
  }


  @Test
  public void test02() {
    String strValue = "{\"threat_list\": [{\"threat_time\":\"1520512804626\",\"threat_id\":\"e8a7e9cb-b40b-361e-86ab-3b3b50ff9b34_369_386_e_speed_1520541739683_37158\",\"threat_type\":\"e_speed\"},{\"threat_time\":\"1520512810855\",\"threat_id\":\"e8a7e9cb-b40b-361e-86ab-3b3b50ff9b34_369_387_e_speed_1520541743630_31995\",\"threat_type\":\"e_speed\"},{\"threat_time\":\"1520512851498\",\"threat_id\":\"e8a7e9cb-b40b-361e-86ab-3b3b50ff9b34_369_391_e_speed_1520541780012_5461\",\"threat_type\":\"e_speed\"},{\"threat_time\":\"1520512875445\",\"threat_id\":\"e8a7e9cb-b40b-361e-86ab-3b3b50ff9b34_369_393_e_speed_1520541799608_40155\",\"threat_type\":\"e_speed\"}],\"server_time\": 1519806010287}";

    JSONObject json = JSONObject.parseObject(strValue);
    JSONArray jsonArray = json.getJSONArray("threat_list");
    String strValue2 = "[{\"threat_time\":\"123\",\"threat_id\":\"aaaaa\",\"threat_type\":\"e_speed\"}]";
    JSONArray jsonArrayResult = JSONArray.parseArray(strValue2);

    jsonArray.addAll(jsonArrayResult);

    System.out.println(json.toJSONString());

  }

  @Test
  public void test03() {

    String str = "{\"protol_version\":1,\"protol_type\":\"userdata\",\"user_data\":[{\"ccb_data\":[{\"id\":1,\"id_card\":\"\",\"phone\":\"\",\"bank_subbranch\":\"\"},{\"id\":2,\"id_card\":\"\",\"phone\":\"\",\"bank_subbranch\":\"\"}]},{\"aa\":\"11\"}]}";

    JSONObject jsonData = JSONObject.parseObject(str);

    if (jsonData.containsKey("user_data") && jsonData.get("user_data") instanceof JSONArray) {



      List<JSONObject> jsonObjectList = jsonData.getJSONArray("user_data").toJavaList(JSONObject.class);
      for (JSONObject object : jsonObjectList) {
        if (object.containsKey("ccb_data") && object.get("ccb_data") instanceof JSONArray) {

          System.out.println(object.getJSONArray("ccb_data").toJSONString());

        }
      }
    }
  }
}
