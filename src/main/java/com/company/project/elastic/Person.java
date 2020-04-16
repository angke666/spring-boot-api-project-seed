package com.company.project.elastic;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * @ClassName Person
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/14 15:32
 * @Version 1.0
 **/
@Data
public class Person extends ElasticBase {

    private Long id;
    private String name;
    private int age;
    private String motto;
    private String phone;
    private double money;
    private String address;
    private String lon;
    private String lat;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date birthDay;
    private boolean disabled;

    private School school;
    private List<Job> jobList;

}
