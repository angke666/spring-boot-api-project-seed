package com.company.project.elastic;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName ElasticServiceImpl
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/15 10:57
 * @Version 1.0
 **/
@Slf4j
@Service("personIndex")
public class ElasticIndexPersonServiceImpl implements IElasticIndexService<Person> {
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public ElasticIndexPersonServiceImpl(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public boolean exists(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);

        return restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
    }

    @Override
    public Map<String, Object> createMappings(Person person) throws IOException {
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

        return mappings;
    }

    @Override
    public GetMappingsResponse getMappings(Person person) throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(person.getIndex());

        return restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
    }

    @Override
    public GetIndexResponse getIndex(Person person) throws IOException {
        GetIndexRequest request = new GetIndexRequest(person.getIndex());
        request.includeDefaults(true);
//        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        return restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);
    }

    @Override
    public void create(Person person) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(person.getIndex());
        Map<String, Object> mappings = createMappings(person);
        request.mapping(mappings);

        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    @Override
    public void update(Person person) throws IOException {
        PutMappingRequest request = new PutMappingRequest(person.getIndex());
        Map<String, Object> mappings = createMappings(person);
        request.source(mappings);
    }

    @Override
    public void delete(Person person) {
        DeleteIndexRequest request = new DeleteIndexRequest(person.getIndex());
        // 设置IndicesOptions控制如何解决不可用的索引以及如何扩展通配符表达式（经过测试，这段代码的作用：即使删除的index不存在，也不会报错）
//        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        try {
            restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.error("Index不存在：" + person.getIndex());
            }
        } catch (Exception e) {
            log.error("删除Index失败：", e);
        }
    }
}
