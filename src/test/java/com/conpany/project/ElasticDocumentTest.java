package com.conpany.project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.company.project.elastic.IElasticDocumentService;
import com.company.project.elastic.Job;
import com.company.project.elastic.Person;
import com.company.project.elastic.School;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName ElasticDocumentTest
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/16 11:20
 * @Version 1.0
 **/
@Slf4j
public class ElasticDocumentTest extends Tester {

    @Resource(name = "personDocument")
    private IElasticDocumentService<Person> service;
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void createDocument() throws ParseException {
        Person person = getPerson();

        try {
            boolean exists = service.exists(person);
            if (exists) {
                log.error("已经存在");
                return;
            }
            IndexResponse response = service.create(person);

            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("已经创建");
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("已经修改");
            }

            String index = response.getIndex();
            String id = response.getId();
            log.info("index:" + index + "|id:" + id);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                log.error("版本冲突错误", e);
            }
            log.error("添加错误", e);
        } catch (Exception e) {
            log.error("程序异常");
        }
    }

    @Test
    public void updateDocument() throws ParseException {
        Person person = getPerson3();

        try {
            service.update(person);
        } catch (IOException e) {
            log.error("修改异常", e);
        }
    }

    @Test
    public void deleteDocument() {
        Person person = new Person();
        person.setIndex("person");
        person.setId(1L);
        try {
            DeleteResponse response = service.delete(person);
            DocWriteResponse.Result result = response.getResult();
            log.info(result.name());
        } catch (IOException e) {
            log.error("删除失败index：" + person.getIndex() + "|id：" + person.getId());
        }
    }

    @Test
    public void getDocument() {
        Person person = new Person();
        person.setIndex("person");
        person.setId(1L);

        try {
            GetResponse response = service.getDocument(person);
            if (response.isExists()) {
                Map<String, Object> map = response.getSourceAsMap();
                log.info("info -> " + map.toString());
            } else {
                log.error("no info");
            }
        } catch (IOException e) {
            log.error("查询失败", e);
        }
    }

    @Test
    public void search() throws IOException {
        BoolQueryBuilder boolQuery = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.matchAllQuery())
//                    .must(QueryBuilders.matchQuery("address", "四川省"))
                // 在我们使用match query时，默认的操作是OR 因为查询的text会根据分词模式拆分相应的形式，（如果不指定分词的模式的话，默认是把中文拆分成一个字的）所以只要有一个字匹配上了就会查出来，所以有3种解决
                // 1.指定为and查询（name的类型是text并且没有指定分词类型 text和name都会被拆分一个字）
//                    .must(QueryBuilders.matchQuery("name", "我大你人张啊").operator(Operator.AND))
                // 2.指定至少匹配几个字（address的类型是text并且没有指定分词类型）
//                    .must(QueryBuilders.matchQuery("school.name", "我大你人张啊").minimumShouldMatch("3"))
                // 3.查询的字段配置映射时指定分词模式analyzer(ik_smart, ik_max_word)或者字段类型为keyword（address指定分词模式是ik_smart，text和address都会按照ik_smart拆分）
//                    .must(QueryBuilders.matchQuery("address", "四川省温州市"))
                // 嵌套查询方法，就是查询的字段类型为nested
//                    .must(QueryBuilders.nestedQuery("jobList",
//                            QueryBuilders
//                                    .boolQuery()
//                                        .must(QueryBuilders.matchQuery("jobList.type", "外卖"))
//                                        .must(QueryBuilders.matchQuery("jobList.name", "饿了么"))
//                            ,ScoreMode.Max))
                // 应对这种情况：在很多的情况下，我们并胡知道是哪一个field含有这个关键词
//                    .must(QueryBuilders.multiMatchQuery("南充", new String[]{"address", "school.address", "name"}))
                // es对地理位置的查询非常强大，不光可以查距离 https://www.elastic.co/guide/en/elasticsearch/client/java-api/7.3/java-geo-queries.html
                .filter(QueryBuilders
                        // pin.字段名
                        .geoDistanceQuery("location")
                        // 坐标
                        .point(31.657401, 106.082974)
                        // 距离
                        .distance(100, DistanceUnit.KILOMETERS))
                ;

        // 查询和过滤的区别，本人理解是由于很多复杂条件导致查询很费时，所以可以先查询简单条件的数据，最后用来过滤最后的数据，节省性能
        // 类似于数据库查询，最后通过代码再处理掉一些数据，缓解数据库压力提升性能
        // 还有很多查询 https://blog.csdn.net/UbuntuTouch/article/details/99546568

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("birthDay", SortOrder.DESC);

        SearchRequest searchRequest = new SearchRequest("person");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits().value;
        SearchHit[] searchHits = hits.getHits();

        List<Object> list = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            Map<String, Object> map = hit.getSourceAsMap();
            list.add(map);
        }
        String json = JSON.toJSONString(list);
        System.out.println("total:" + total);
        System.out.println(json);
    }

    @Test
    public void createBulkDocument() throws ParseException {
        Person person1 = getPerson();
        Person person2 = getPerson2();
        Person person3 = getPerson3();

        List<Person> list = new ArrayList<>();
        list.add(person1);
        list.add(person2);
        list.add(person3);
        service.createBulk(list);

        try {
            // 为了测试时能获取异步执行的日志
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            log.error("线程休眠异常", e);
        }
    }

    public Person getPerson() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date birthDay = sdf.parse("1949-10-01 10:08:08");

        Person person = new Person();
        person.setIndex("person");
        person.setId(1L);
        person.setName("张三");
        person.setAge(22);
        person.setMotto("我和我的祖国，爱喝苹果汁，电脑手机华为平板");
        person.setPhone("13888888888");
        person.setMoney(999.36);
        person.setAddress("中华人民共和国四川省成都市高新区天府广场");
        person.setLon(104.065861);
        person.setLat(30.657401);
        person.setBirthDay(birthDay);
        person.setDisabled(false);

        School school = new School();
        school.setName("四川大学");
        school.setAge(90);
        school.setAddress("四川省成都市锦江区大学路");

        Job job1 = new Job();
        job1.setName("班长");
        job1.setType("学生");

        Job job2 = new Job();
        job2.setName("美团外卖");
        job2.setType("外卖小哥");

        List<Job> jobList = new ArrayList<>();
        jobList.add(job1);
        jobList.add(job2);

        person.setSchool(school);
        person.setJobList(jobList);

        return person;
    }

    public Person getPerson2() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date birthDay = sdf.parse("1996-05-22 10:08:08");

        Person person = new Person();
        person.setIndex("person");
        person.setId(2L);
        person.setName("李四");
        person.setAge(33);
        person.setMotto("喜欢旅游，苹果小米，手机电视");
        person.setPhone("13666666666");
        person.setMoney(1225.22);
        person.setAddress("中华人民共和国四川省南充市嘉陵区宝岛路");
        person.setLon(106.082974);
        person.setLat(30.795282);
        person.setBirthDay(birthDay);
        person.setDisabled(false);

        School school = new School();
        school.setName("四川大学南充学院");
        school.setAge(60);
        school.setAddress("四川省南充市嘉陵区大学路");

        Job job1 = new Job();
        job1.setName("组长");
        job1.setType("学生");

        Job job2 = new Job();
        job2.setName("饿了么外卖");
        job2.setType("外卖小哥");

        List<Job> jobList = new ArrayList<>();
        jobList.add(job1);
        jobList.add(job2);

        person.setSchool(school);
        person.setJobList(jobList);

        return person;
    }

    public Person getPerson3() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date birthDay = sdf.parse("2001-08-22 03:55:11");

        Person person = new Person();
        person.setIndex("person");
        person.setId(3L);
        person.setName("王五");
        person.setAge(67);
        person.setMotto("喜欢看电影，中兴华为锤子，手表手机");
        person.setPhone("13555555555");
        person.setMoney(999.36);
        person.setAddress("中华人民共和国浙江省温州市高新区路桥大街");
        person.setLon(120.672111);
        person.setLat(28.000575);
        person.setBirthDay(birthDay);
        person.setDisabled(false);

        School school = new School();
        school.setName("温州大学");
        school.setAge(55);
        school.setAddress("浙江省温州市高新区学府路");

        Job job1 = new Job();
        job1.setName("学习委员");
        job1.setType("学生");

        Job job2 = new Job();
        job2.setName("滴滴");
        job2.setType("快递小哥");

        List<Job> jobList = new ArrayList<>();
        jobList.add(job1);
        jobList.add(job2);

        person.setSchool(school);
        person.setJobList(jobList);

        return person;
    }

}
