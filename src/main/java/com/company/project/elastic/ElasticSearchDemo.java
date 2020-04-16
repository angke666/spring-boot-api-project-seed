package com.company.project.elastic;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName ElasticSearchDemo
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/14 10:16
 * @Version 1.0
 **/
public class ElasticSearchDemo {

    public static void main(String[] args) {
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        Map<String, Object> age = new HashMap<>();
        age.put("type", "integer");
        Map<String, Object> motto = new HashMap<>();
        motto.put("type", "text");
        motto.put("analyzer", "ik_max_word");
        motto.put("search_analyzer", "ik_max_word");
        Map<String, Object> phone = new HashMap<>();
        phone.put("type", "keyword");
        Map<String, Object> money = new HashMap<>();
        money.put("type", "double");
        Map<String, Object> address = new HashMap<>();
        // 可以对一个字段提供多种索引模式，同一个字段的值，一个分词，一个不分词
        // 那么以后搜索过滤和排序就可以使用address.keyword字段名
        Map<String, String> addressFieldsTypeMap = new HashMap<>();
        addressFieldsTypeMap.put("type", "keyword");
        // 超过150个字符的文本，将会被忽略，不被索引
        addressFieldsTypeMap.put("ignore_above", "150");
        Map<String, Map<String, String>> addressFieldsMap = new HashMap<>();
        addressFieldsMap.put("keyword", addressFieldsTypeMap);
        address.put("type", "text");
        address.put("analyzer", "ik_smart");
        address.put("search_analyzer", "ik_smart");
        address.put("fields", addressFieldsMap);
        Map<String, Object> location = new HashMap<>();
        location.put("type", "geo_point");
        Map<String, Object> birthDay = new HashMap<>();
        birthDay.put("type", "date");
        birthDay.put("format", "yyyy-MM-dd HH:mm:ss");
        Map<String, Object> disabled = new HashMap<>();
        disabled.put("type", "boolean");
        Map<String, Object> school = new HashMap<>();
        school.put("type", "object");
        Map<String, Object> jobList = new HashMap<>();
        jobList.put("type", "nested");

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", name);
        properties.put("age", age);
        properties.put("motto", motto);
        properties.put("phone", phone);
        properties.put("money", money);
        properties.put("address", address);
        properties.put("location", location);
        properties.put("birthDay", birthDay);
        properties.put("disabled", disabled);
        properties.put("school", school);
        properties.put("jobList", jobList);

        Map<String, Object> mappings = new HashMap<>();
        mappings.put("properties", properties);

    }

}
