package org.fire.datasync.task;

import com.mongodb.*;
import org.bson.types.BSONTimestamp;
import org.fire.datasync.common.Lifecycle;
import org.fire.datasync.executor.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * User: fire
 * Date: 2018-01-13
 */
public class OPLogSyncTask implements Runnable, Lifecycle {
    private static final Logger log = LoggerFactory.getLogger(OPLogSyncTask.class);

    // 读取oplog时需要忽略带这种标识的数据，因为这仅代表数据迁移不代表真实数据操作
    public static final String MONGODB_SHARDING_FLAG = "fromMigrate";
    // mongodb 自然排序标识
    public static final String MONGODB_NATURE_OP = "$natural";
    // oplog时间戳字段
    public static final String OPLOG_TIMESTAMP = "ts";
    // oplog namespace字段
    public static final String NAMESPACE = "ns";
    // oplog数据操作类型字段
    public static final String OP = "op";
    public static final String OP_INSERT = "INSERT";
    public static final String OP_UPDATE = "UPDATE";
    public static final String OP_DELETE = "DELETE";

    private volatile boolean running;
    private DBCollection oplogCollection;
    private BSONTimestamp timestamp;
    // 数据消费者
    private Consumer<OPLogMessage> consumer;

    /**
     * @param collection
     * @param consumer
     */
    public OPLogSyncTask(DBCollection collection, Consumer<OPLogMessage> consumer) {
        this.oplogCollection = Objects.requireNonNull(collection, "collection can not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer can not be null");
    }

    private void initialize() {
        running = true;
        startScheduleSaveTimestamp();
    }

    @Override
    public void start() {
        initialize();
    }

    @Override
    public void stop() {
        running = false;
    }

    /**
     * 启动定时任务保存timestamp
     */
    private void startScheduleSaveTimestamp() {
        int period = 1;
        Runnable task = () -> {
            try {
                if (timestamp != null) {
                    Timestamps.set(timestamp);
                }
            } catch (Throwable t) {
                log.error("failed to update timestamp", t);
            }
        };
        Schedulers.INSTANCE.schedule(task, period, period, TimeUnit.SECONDS);
        log.info("start update timestamp, period={}", period);
    }

    @Override
    public void run() {
        while (running) {
            try (DBCursor cursor = oplogCursor(getTimestamp())) {
                while (cursor.hasNext() && running) {
                    process(cursor.next());
                }
                logAndSleep();
            } catch (Throwable t) {
                if (running) {
                    log.error("process error", t);
                }
            }
        }
    }

    /**
     * 从指定timestamp处开始拉取oplog
     * <p>
     * 请注意，以TAILABLE方式获取的DBCursor在mongodb driver
     * 内部是以一个while循环不停发送getmore命令到mongodb实现的
     *
     * @param timestamp oplog起始时间
     * @return 一个持续等待数据、不超时的DBCursor
     */
    private DBCursor oplogCursor(BSONTimestamp timestamp) {
        DBObject query = new BasicDBObject();
        query.put(OPLOG_TIMESTAMP, new BasicDBObject(QueryOperators.GT, timestamp));
        int options = Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA
                | Bytes.QUERYOPTION_NOTIMEOUT | Bytes.QUERYOPTION_OPLOGREPLAY;
        return oplogCollection.find(query).setOptions(options);
    }

    /**
     * 获取时间戳，首先获取最近一次保存的值，
     * 获取不到则从mongodb中获取最新的值，仍旧获取不到则以当前时刻作为起始值
     *
     * @return
     */
    private BSONTimestamp getTimestamp() {
        BSONTimestamp timestamp = Timestamps.get();
        if (timestamp == null) {
            timestamp = getLatestTimestamp();
        }
        if (timestamp == null) {
            timestamp = initializeTimestamp();
        }
        this.timestamp = timestamp;
        return timestamp;
    }

    /**
     * 取oplog中最新一条记录的ts作为时间戳
     *
     * @return
     */
    private BSONTimestamp getLatestTimestamp() {
        BSONTimestamp timestamp = null;
        DBObject sort = new BasicDBObject(MONGODB_NATURE_OP, -1);
        try (DBCursor cursor = oplogCollection.find().sort(sort).limit(1)) {
            while (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                timestamp = (BSONTimestamp) dbObject.get(OPLOG_TIMESTAMP);
                break;
            }
        }
        return timestamp;
    }

    /**
     * 以当前时刻作为timestamp
     *
     * @return
     */
    private BSONTimestamp initializeTimestamp() {
        int time = (int) (System.currentTimeMillis() / 1000);
        int incr = 0;
        return new BSONTimestamp(time, incr);
    }

    private void logAndSleep() throws InterruptedException {
        log.warn("cursor returned without data, rebuild cursor");
        TimeUnit.MILLISECONDS.sleep(500);
    }

    /**
     * 处理一条oplog数据
     *
     * @param dbObject
     */
    private void process(DBObject dbObject) {
        if (dbObject.containsField(MONGODB_SHARDING_FLAG)) {
            return;
        }

        String ns = (String) dbObject.get(NAMESPACE);
        String op = (String) dbObject.get(OP);

        if (!"i".equals(op) && !"u".equals(op) && !"d".equals(op)) {
            return;
        }

        String[] arr = ns.split("\\.");
        if (arr.length < 2) {
            log.warn("wrong format of ns:{}", ns);
            return;
        }
        String database = arr[0];
        String collection = arr[1];

        DBObject data = null;
        String opType = null;
        if ("i".equals(op)) { // 插入
            data = (DBObject) dbObject.get("o");
            opType = OP_INSERT;
        } else if ("u".equals(op)) { // 更新
            data = (DBObject) dbObject.get("o2");
            opType = OP_UPDATE;
        } else if ("d".equals(op)) { // 删除
            data = (DBObject) dbObject.get("o");
            opType = OP_DELETE;
        }

        if (data != null && opType != null) {
            dispatch(database, collection, data, opType);
        }

        updateTimestamp(dbObject);
    }

    /**
     * 将数据发往消费者
     *
     * @param database
     * @param collection
     * @param data
     * @param op
     */
    private void dispatch(String database, String collection, DBObject data, String op) {
        OPLogMessage message = new OPLogMessage();
        message.setDatabase(database);
        message.setCollection(collection);
        message.setType(op);
        message.setData(data);

        consumer.accept(message);
    }

    private void updateTimestamp(DBObject dbObject) {
        this.timestamp = (BSONTimestamp) dbObject.get(OPLOG_TIMESTAMP);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(OPLogSyncTask.class.getSimpleName())
                .append("running=").append(running)
                .append(",connection=").append(oplogCollection.getDB().getMongo().getAllAddress())
                .append(",timestamp=").append(timestamp)
                .append("]");
        return sb.toString();
    }

    /**
     * 以本地文件的形式存取时间戳
     */
    private static class Timestamps {
        private static final String tsTmpFile = "oplog.ts";

        static BSONTimestamp get() {
            BSONTimestamp timestamp = null;
            Path path = Paths.get(tsTmpFile);
            if (!path.toFile().exists()) {
                return null;
            }
            Charset cs = StandardCharsets.UTF_8;
            try {
                List<String> lines = Files.readAllLines(path, cs);
                if (lines.size() > 0) {
                    String line = lines.get(0);
                    String[] arr = line.split(":");
                    if (arr.length == 2) {
                        int time = Integer.parseInt(arr[0]);
                        int incr = Integer.parseInt(arr[1]);
                        timestamp = new BSONTimestamp(time, incr);
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            return timestamp;
        }

        static void set(BSONTimestamp timestamp) {
            int time = timestamp.getTime();
            int incr = timestamp.getInc();
            String strValue = String.format("%d:%d", time, incr);
            Path path = Paths.get(tsTmpFile);
            try {
                Files.write(path, strValue.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }
}
