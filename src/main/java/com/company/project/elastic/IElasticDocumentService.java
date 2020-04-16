package com.company.project.elastic;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName IElasticDocumentService
 * @Description TODO
 * @Author 钱进
 * @Date 2020/4/16 10:33
 * @Version 1.0
 **/
public interface IElasticDocumentService<T extends ElasticBase> {

    /**
     * 判断document是否存在
     * @param entity
     * @return
     */
    boolean exists(T entity) throws IOException;

    /**
     * 普通获取一条document（通过index和id）
     * @param entity
     * @return
     */
    GetResponse getDocument(T entity) throws IOException;

    /**
     * 新增
     * @param entity
     * @return
     */
    IndexResponse create(T entity) throws IOException;

    void createBulk(List<T> entityList);

    /**
     * 修改
     * @param entity
     * @return
     */
    UpdateResponse update(T entity) throws IOException;

    /**
     * 删除
     * @param entity
     * @return
     */
    DeleteResponse delete(T entity) throws IOException;

}
