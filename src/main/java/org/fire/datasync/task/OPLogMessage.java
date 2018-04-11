package org.fire.datasync.task;

import com.mongodb.DBObject;
import org.bson.types.BSONTimestamp;

/**
 * mongodb变化信息
 * User: fire
 * Date: 2018-01-13
 */
public class OPLogMessage {
    // 数据操作类型：INSERT、UPDATE、DELETE
    private String type;
    // 数据库实例
    private String instance;
    // 数据所在数据库名
    private String database;
    // 数据所在表名
    private String collection;
    // 有效数据
    // 注意：更新、删除操作这里存放的是操作条件
    private DBObject data;

    public OPLogMessage copy() {
        OPLogMessage copy = new OPLogMessage();
        copy.data = this.data;
        copy.database = this.database;
        copy.collection = this.collection;
        copy.type = this.type;
        return copy;
    }

    public DBObject getData() {
        return data;
    }

    public void setData(DBObject data) {
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public BSONTimestamp getTimestamp() {
        return data != null ? (BSONTimestamp) data.get(OPLogSyncTask.OPLOG_TIMESTAMP) : null;
    }
}
