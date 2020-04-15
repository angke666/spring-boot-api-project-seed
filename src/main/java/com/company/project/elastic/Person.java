package com.company.project.elastic;

import lombok.Data;

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
    private String number;

}
