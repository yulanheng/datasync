package org.fire.datasync.task;

import com.mongodb.*;
import org.fire.datasync.common.Lifecycle;
import org.fire.datasync.executor.SingleThreadExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * User: fire
 * Date: 2018-04-08
 */
public class DispatcherConsumer implements Consumer<OPLogMessage>, Lifecycle {
    private static final Logger log = LoggerFactory.getLogger(DispatcherConsumer.class);

    private MongoClient mongoClient;
    private ConcurrentMap<String, DBCollection> collectionMap = new ConcurrentHashMap<>();
    private static ConcurrentMap<String, SingleThreadExecutor> executors = new ConcurrentHashMap<>();

    /**
     * 如果操作类型是update则先根据where条件执行查询
     * 然后迭代查询结果
     *
     * @param message
     */
    @Override
    public final void accept(OPLogMessage message) {
        Executor executor = getExecutor(message);
        Runnable task = () -> {
            if (OPLogSyncTask.OP_UPDATE.equals(message.getType())) {
                DBCollection collection = getCollection(message.getDatabase(), message.getCollection());
                try (DBCursor cursor = collection.find(message.getData())) {
                    while (cursor.hasNext()) {
                        OPLogMessage oneMessage = message.copy();
                        DBObject item = cursor.next();
                        item.put("_id", item.get("_id").toString());
                        oneMessage.setData(item);
                        onMessage(oneMessage);
                    }
                }
            } else {
                onMessage(message);
            }
        };
        executor.execute(task);
    }

    protected void onMessage(OPLogMessage message) {
        log.debug("onMessage>>>{}", message);
    }

    private SingleThreadExecutor getExecutor(OPLogMessage message) {
        String key = determinConcurrencyLevel(message);
        if (!executors.containsKey(key)) {
            synchronized (executors) {
                if (!executors.containsKey(key)) {
                    SingleThreadExecutor threadExecutor = new SingleThreadExecutor(key, 1000);
                    executors.put(key, threadExecutor);
                }
            }
        }
        return executors.get(key);
    }

    /**
     * 决定分发线程级别
     *
     * @param message
     * @return
     */
    protected String determinConcurrencyLevel(OPLogMessage message) {
//        return message.getInstance() + ":" + message.getDatabase() + ":" + message.getCollection();
        return message.getInstance();
    }

    private DBCollection getCollection(String dbname, String collection) {
        DBCollection dbCollection = getFromCache(dbname, collection);
        if (dbCollection == null) {
            DB db = mongoClient.getDB(dbname);
            dbCollection = db.getCollection(collection);
            collectionMap.put(dbname + collection, dbCollection);
        }
        return dbCollection;
    }

    private DBCollection getFromCache(String dbname, String collection) {
        DBCollection dbCollection = null;
        if (mongoClient != null) {
            String key = dbname + collection;
            dbCollection = collectionMap.get(key);
        }
        return dbCollection;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public void close() {
        for (SingleThreadExecutor executor : executors.values()) {
            executor.stop();
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        close();
    }
}
