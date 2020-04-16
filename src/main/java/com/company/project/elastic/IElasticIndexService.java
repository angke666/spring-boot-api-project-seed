package com.company.project.elastic;

import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.GetMappingsResponse;

import java.io.IOException;
import java.util.Map;

/**
 * @author 钱进
 */
public interface IElasticIndexService<T extends ElasticBase> {

    /**
     * 判断index是否存在
     * @param index
     * @return
     * @throws IOException
     */
    boolean exists(String index) throws IOException;

    /**
     * 创建index的字段映射
     * @param entity
     * @return
     * @throws IOException
     */
    Map<String, Object> createMappings(T entity) throws IOException;

    /**
     * 获取index的字段映射配置
     * @param entity
     * @return
     * @throws IOException
     */
    GetMappingsResponse getMappings(T entity) throws IOException;

    /**
     * 获取index信息
     * @param entity
     * @return
     * @throws IOException
     */
    GetIndexResponse getIndex(T entity) throws IOException;

    /**
     * 创建index
     * @param entity
     * @throws IOException
     */
    void create(T entity) throws IOException;

    /**
     * 修改index（主要就是修改字段的映射配置）
     * @param entity
     * @throws IOException
     */
    void update(T entity) throws IOException;

    /**
     * 删除index
     * @param entity
     */
    void delete(T entity);
}
