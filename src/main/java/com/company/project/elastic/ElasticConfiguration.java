package com.company.project.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ElasticConfiguration
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/14 15:38
 * @Version 1.0
 **/
@Configuration
public class ElasticConfiguration {

    @Value("${elastic.hosts}")
    private String hosts;
    @Value("${elastic.port}")
    private int port;
    @Value("${elastic.schema}")
    private String schema;
    @Value("${elastic.connectTimeOut}")
    private int connectTimeOut;
    @Value("${elastic.socketTimeOut}")
    private int socketTimeOut;
    @Value("${elastic.connectionRequestTimeOut}")
    private int connectionRequestTimeOut;
    @Value("${elastic.maxConnectNum}")
    private int maxConnectNum;
    @Value("${elastic.maxConnectPerRoute}")
    private int maxConnectPerRoute;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        List<HttpHost> hostList = new ArrayList<>();
        String[] hostArr = hosts.split(",");
        for (String host : hostArr) {
            hostList.add(new HttpHost(host, port, schema));
        }

        RestClientBuilder builder = RestClient.builder(hostList.toArray(new HttpHost[0]));

        // 异步httpClient连接时配置
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(connectTimeOut);
            requestConfigBuilder.setSocketTimeout(socketTimeOut);
            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
            return requestConfigBuilder;
        });

        // 异步httpClient连接数量配置
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
           httpClientBuilder.setMaxConnTotal(maxConnectNum);
           httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
           return httpClientBuilder;
        });

        return new RestHighLevelClient(builder);
    }

}
