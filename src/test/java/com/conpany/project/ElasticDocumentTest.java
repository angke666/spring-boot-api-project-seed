package com.conpany.project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.company.project.elastic.IElasticDocumentService;
import com.company.project.elastic.Job;
import com.company.project.elastic.Person;
import com.company.project.elastic.School;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

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
        Person person = getPerson();

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
    public void createBulkDocument() throws ParseException {
        Person person2 = getPerson2();
        Person person3 = getPerson3();

        List<Person> list = new ArrayList<>();
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
        person.setLon("104.065861");
        person.setLat("30.657401");
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
        person.setLon("106.082974");
        person.setLat("30.795282");
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
        person.setLon("120.672111");
        person.setLat("28.000575");
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
