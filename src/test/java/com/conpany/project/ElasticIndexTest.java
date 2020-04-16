package com.conpany.project;

import com.company.project.elastic.IElasticIndexService;
import com.company.project.elastic.Person;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

/**
 * @ClassName ElasticSearchTest
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/15 10:21
 * @Version 1.0
 **/
@Slf4j
public class ElasticIndexTest extends Tester{

    @Resource(name = "personIndex")
    private IElasticIndexService<Person> service;

    @Test
    public void createIndex() {
        Person person = new Person();
        person.setIndex("person");

        try {
            boolean exists = service.exists(person.getIndex());
            if (!exists) {
                service.create(person);
                log.info("创建Index成功");
            } else {
                log.info(person.getIndex() + "已经存在");
            }
        } catch (Exception e) {
            log.error("创建Index异常", e);
        }
    }

    @Test
    public void deleteIndex() {
        Person person = new Person();
        person.setIndex("person");

        service.delete(person);
    }

    @Test
    public void getIndex() {
        Person person = new Person();
        person.setIndex("person");
        try {
            GetIndexResponse response = service.getIndex(person);
            Map<String, MappingMetaData> mappings = response.getMappings();
            for (String key : mappings.keySet()) {
                MappingMetaData mappingMetaData = mappings.get(key);
                CompressedXContent source = mappingMetaData.source();
                String toString = source.toString();
                System.out.println(toString);
            }
        } catch (Exception e) {
            log.error("获取index异常", e);
        }
    }

}
