package org.fire.datasync.task;

import com.mongodb.DBObject;
import org.bson.types.BSONTimestamp;

/**
 * mongodb变化信息
 * User: fire
 * Date: 2018-01-13
 */
public class OPLogMessage implements Cloneable {
    // 数据操作类型：INSERT、UPDATE、DELETE
    private String type;
    // 数据所在数据库名
    private String database;
    // 数据所在表名
    private String collection;
    // 有效数据
    // 注意：更新、删除操作这里存放的是操作条件
    private DBObject data;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OPLogMessage=[")
                .append("type=").append(type)
                .append(",database=").append(database)
                .append(",collection=").append(collection)
                .append(",data=").append(data)
                .append("]");
        return sb.toString();
    }

    @Override
    public OPLogMessage clone() {
        try {
            return (OPLogMessage) super.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }
    }
}
