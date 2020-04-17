package com.company.project.elastic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName ElasticDocumentPersonImpl
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/16 10:51
 * @Version 1.0
 **/
@Slf4j
@Service("personDocument")
public class ElasticDocumentPersonImpl implements IElasticDocumentService<Person> {
    private final RestHighLevelClient restHighLevelClient;

    public ElasticDocumentPersonImpl(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public boolean exists(Person entity) throws IOException {
        GetRequest request = new GetRequest(entity.getIndex(), entity.getId().toString());

        return restHighLevelClient.exists(request, RequestOptions.DEFAULT);
    }

    @Override
    public GetResponse getDocument(Person entity) throws IOException {
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.3/java-rest-high-document-get.html 接口文档可以设置更多参数
        GetRequest request = new GetRequest(entity.getIndex(), entity.getId().toString());
        // 查询前先刷新
        request.refresh(true);
        // 可以查询指定版本
//        request.version(1);
//        request.versionType(VersionType.EXTERNAL);

        return restHighLevelClient.get(request, RequestOptions.DEFAULT);
    }

    @Override
    public IndexResponse create(Person entity) throws IOException {
        Map<String, Object> location = new HashMap<>();
        location.put("lon", entity.getLon());
        location.put("lat", entity.getLat());
        JSONObject jsonObject = JSONObject.parseObject(JSON.toJSONString(entity));
        jsonObject.remove("lon");
        jsonObject.remove("lat");
        jsonObject.remove("id");
        jsonObject.put("location", location);

        IndexRequest request = new IndexRequest(entity.getIndex());
        request.id(entity.getId().toString());
        request.source(JSON.toJSONString(jsonObject), XContentType.JSON);

        // 操作类型有两种策略 create和index（默认）
        // create：如果该document存在，保存时会报错
        // index：如果该document存在，保存时不会报错，因为每次保存都是覆盖保存
        request.opType(DocWriteRequest.OpType.CREATE);
        request.opType("create");

        // 之前的每个方法，都提供了异步操作的，这里只是为了测试简便，全部使用的同步
        return restHighLevelClient.index(request, RequestOptions.DEFAULT);
    }

    @Override
    public void createBulk(List<Person> entityList) {
        BulkRequest bulkRequest = new BulkRequest();
        for (Person entity : entityList) {
            Map<String, Object> location = new HashMap<>();
            location.put("lon", entity.getLon());
            location.put("lat", entity.getLat());
            JSONObject jsonObject = JSONObject.parseObject(JSON.toJSONString(entity));
            jsonObject.remove("lon");
            jsonObject.remove("lat");
            jsonObject.remove("id");
            jsonObject.put("location", location);
            IndexRequest indexRequest = new IndexRequest(entity.getIndex());
            indexRequest.id(entity.getId().toString());
            indexRequest.source(JSON.toJSONString(jsonObject), XContentType.JSON);
            indexRequest.opType(DocWriteRequest.OpType.CREATE);

            bulkRequest.add(indexRequest);
        }
        // 配置超时时间 2分钟
        bulkRequest.timeout("2m");

        restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                log.info("批量添加成功");
            }

            @Override
            public void onFailure(Exception e) {
                log.error("批量添加失败", e);
            }
        });
    }

    @Override
    public UpdateResponse update(Person entity) throws IOException {
        UpdateRequest request = new UpdateRequest(entity.getIndex(), entity.getId().toString());
        Map<String, Object> location = new HashMap<>();
        location.put("lon", entity.getLon());
        location.put("lat", entity.getLat());
        JSONObject jsonObject = JSONObject.parseObject(JSON.toJSONString(entity));
        jsonObject.remove("lon");
        jsonObject.remove("lat");
        jsonObject.remove("id");
        jsonObject.put("location", location);
        request.doc(JSON.toJSONString(jsonObject), XContentType.JSON);

        return restHighLevelClient.update(request, RequestOptions.DEFAULT);
    }

    @Override
    public DeleteResponse delete(Person entity) throws IOException {
        DeleteRequest request = new DeleteRequest(entity.getIndex(), entity.getId().toString());

        return restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }
}
