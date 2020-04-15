package com.conpany.project;

import com.company.project.elastic.IElasticIndexService;
import com.company.project.elastic.Person;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Resource;

/**
 * @ClassName ElasticSearchTest
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/15 10:21
 * @Version 1.0
 **/
@Slf4j
public class ElasticSearchTest extends Tester{

    @Resource(name = "person")
    private IElasticIndexService<Person> service;

    @Test
    public void elastic() {
        Person person = new Person();
        person.setIndex("person");

        try {
//            boolean exists = service.exists("person");
//            log.info("是否存在：" + exists);

            service.delete(person);
        } catch (Exception e) {
            log.error("查询异常", e);
        }
    }

}
