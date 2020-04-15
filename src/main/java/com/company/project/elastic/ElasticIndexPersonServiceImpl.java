package com.company.project.elastic;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @ClassName ElasticServiceImpl
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/15 10:57
 * @Version 1.0
 **/
@Slf4j
@Service("person")
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
    public XContentBuilder createMappings(Person person) throws IOException {
        // 配置映射
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .startObject("properties")
                    .startArray("message")
                        .field("name", "text")
                    .endObject()
                .endObject()
        .endObject();

        return builder;
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
        XContentBuilder builder = createMappings(person);
        request.mapping(builder);

        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    @Override
    public void update(Person person) throws IOException {
        PutMappingRequest request = new PutMappingRequest(person.getIndex());
        XContentBuilder builder = createMappings(person);
        request.source(builder);
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
