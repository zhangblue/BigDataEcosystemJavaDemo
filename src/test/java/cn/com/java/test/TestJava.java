package cn.com.java.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
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

  @Test
  public void test04() {
    String mac = "00:EC:0A:DB:C0:ED";
//正则校验MAC合法性
    String patternMac = "^[A-F0-9]{2}(:[A-F0-9]{2}){5}$";
    if (Pattern.compile(patternMac).matcher(mac).find()) {
      System.out.println("yes");
    } else {
      System.out.println("no");
    }
  }

  @Test
  public void test05() {
    String line = "{\"server_time\": \"1522720064022\",\"ip_lan\": \"172.16.19.138\",\"agent_id\": 1,\"start_id\": 1522720064,\"app_version\": \"unknown\",\"os_version\": \"unknown\",\"start\": \"{\\\"uid\\\":11229,\\\"uname\\\":\\\"lzh\\\",\\\"net_type\\\":\\\"NETWORK_WIFI\\\",\\\"pname\\\":\\\"lzh\\\",\\\"udid_from\\\":\\\"file\\\",\\\"pid\\\":19486}\",\"ip_src\": \"192.168.138.182\",\"permission\": \"[{\\\"perm_value\\\":true,\\\"perm_name\\\":\\\"INTERNET\\\"},{\\\"perm_value\\\":true,\\\"perm_name\\\":\\\"READ_PHONE_STATE\\\"},{\\\"perm_value\\\":true,\\\"perm_name\\\":\\\"ACCESS_NETWORK_STATE\\\"},{\\\"perm_value\\\":true,\\\"perm_name\\\":\\\"ACCESS_COARSE_LOCATION\\\"}]\",\"version\": \"everisk 1.0 beta\",\"dt_server_time\": \"2018-04-03T09:47:44+08:00\",\"platform\": \"android\",\"dt_time\": \"2018-04-03T09:47:44+08:00\",\"manufacturer\": \"unknown\",\"protol_type\": \"start\",\"os_info\": \"android unknown\",\"extra\": \"{\\\"location\\\":{\\\"latitude\\\":\\\"31.2090626936\\\",\\\"longitude\\\":\\\"121.4934089939\\\"}}\",\"self_md5\": \"1000000000000000000000000000000001\",\"protol_version\": 4,\"app_info\": \"android unknown\",\"time\": 1522720064045,\"udid\": \"3698-00000000-0000-0000-0000-000000003698\",\"msg_id\": 3362}";

    JSONObject jsonObject = JSONObject.parseObject(line);

    if (jsonObject.containsKey("protol_type") && jsonObject.getString("protol_type").equals("start")) {
      System.out.println("1");

      if (jsonObject.containsKey("extra") && jsonObject.getJSONObject("extra") instanceof JSONObject) {
        System.out.println("2");
        JSONObject jsonObjectExtra = jsonObject.getJSONObject("extra");
        if (jsonObjectExtra.containsKey("location") && jsonObjectExtra.get("location") instanceof JSONObject) {
          System.out.println("3");
          JSONObject jsonObjectLocation = jsonObjectExtra.getJSONObject("location");
          if (jsonObjectLocation.containsKey("latitude") && jsonObjectLocation.containsKey("longitude")) {
            System.out.println("4");
            System.out.println(jsonObjectLocation.getString("latitude") + "|" + jsonObjectLocation.getString("longitude"));
          }
        }
      }
    }
  }


  @Test
  public void test06() {
    Calendar calendar = Calendar.getInstance();
    System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
  }
}
