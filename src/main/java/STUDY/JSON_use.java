package STUDY;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;

public class JSON_use {

    public static void main(String[] args) {
        // 学习fastjson的用法
        String log = "{\n" +
                "\"host\":\"www.baidu.com\",\n" +
                "\"user_id\":\"13755569427\",\n" +
                "\"items\":[\n" +
                "    {\n" +
                "        \"item_type\":\"eat\",\n" +
                "        \"active_time\":156234\n" +
                "    },\n" +
                "    {\n" +
                "        \"item_type\":\"car\",\n" +
                "        \"active_time\":156233\n" +
                "    }\n" +
                " ]\n" +
                "}";
        // json {}  [{},{}]
        // 将该json 字符串变为  一个个json 对象
        // 将一个json字符串转换为json 对象
        JSONObject jsonObject = JSON.parseObject(log);
        // 从json 对象中获取值
        String host = jsonObject.getString("host");
        String userId = jsonObject.getString("user_id");
        System.out.println(host);
        System.out.println(userId);
        // 从json 对象中获取json数组
        JSONArray items = jsonObject.getJSONArray("items");

        // 定义一个集合，将来把封装好的json 对象，放入进去
        ArrayList<JSONObject> list = new ArrayList<>();
        // 循环这个Array数组，得到更加详细的数据
        for (Object item : items) {

            // 将一个个json字符串转换为json 对象
            JSONObject obj2 = JSON.parseObject(item.toString());
            String active_time = obj2.getString("active_time");
            String item_type = obj2.getString("item_type");


            // 将解析出来的4个属性，全部放入一个json 对象中
            JSONObject newObj = new JSONObject();

            newObj.put("host", host);
            newObj.put("user_id", userId);
            newObj.put("active_time", active_time);
            newObj.put("item_type", item_type);
            list.add(newObj);
        }
        // 如何将一个集合，变为json 字符串
        String jsonString = JSON.toJSONString(list);
        System.out.println(jsonString);

    }



}
