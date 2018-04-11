package org.fire.datasync.task;

import com.mongodb.*;
import org.bson.types.BSONTimestamp;
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
public class OPLogSyncTask {
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
    private Thread thread;
    private DBCollection oplogCollection;
    private BSONTimestamp timestamp;
    // 数据消费者
    private Consumer<OPLogMessage> consumer;
    // true表示OPLogSyncTask会自动更新oplog的timestamp
    // 为false则需要用户自行更新timestamp
    private boolean autocommit;
    private String instance;

    public OPLogSyncTask(DBCollection collection, Consumer<OPLogMessage> consumer) {
        this.oplogCollection = Objects.requireNonNull(collection, "collection can not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer can not be null");
        this.autocommit = true;
        thread = new Thread(() -> run());
    }

    public void start() {
        running = true;
        thread.start();
        startScheduleSaveTimestamp();
    }

    public void stop() {
        running = false;
        try {
            // oplog以tail方式运行，如果当前没有数据可读则会一直阻塞，
            // 所以这里需要手动interrupt以中断当前行为
            thread.interrupt();
            thread.join();
        } catch (InterruptedException ie) {
            // ignore
        }
    }

    private void startScheduleSaveTimestamp() {
        if (autocommit) {
            int period = 3;
            Runnable task = () -> {
                try {
                    Timestamps.set(timestamp);
                } catch (Throwable t) {
                    log.error("定时保存timestamp出错", t);
                }
            };
            Schedulers.schedule(task, period, period, TimeUnit.SECONDS);
            log.info("启动自动更新timestamp，间隔{}秒", period);
        }
    }

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

    private DBCursor oplogCursor(BSONTimestamp timestamp) {
        DBObject query = new BasicDBObject();
        query.put(OPLOG_TIMESTAMP, new BasicDBObject(QueryOperators.GT, timestamp));
        int options = Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA
                | Bytes.QUERYOPTION_NOTIMEOUT | Bytes.QUERYOPTION_OPLOGREPLAY;
        return oplogCollection.find(query).setOptions(options);
    }

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
            log.warn("oplog ns字段无法区分库和表:{}，允许格式为:database.collection", ns);
            return;
        }

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
            dispatch(arr[0], arr[1], data, opType);
        }

        if (autocommit) {
            updateTimestamp(dbObject);
        }
    }

    private void dispatch(String database, String table, DBObject data, String op) {
        OPLogMessage message = new OPLogMessage();
        message.setDatabase(database);
        message.setCollection(table);
        message.setType(op);
        message.setData(data);

        consumer.accept(message);
    }

    private void updateTimestamp(DBObject dbObject) {
        this.timestamp = (BSONTimestamp) dbObject.get(OPLOG_TIMESTAMP);
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(OPLogSyncTask.class.getSimpleName())
                .append("=[autocommit=").append(autocommit)
                .append(",running=").append(running)
                .append(",collection=").append(oplogCollection.getDB().getMongo().getAddress())
                .append("timestamp=").append(timestamp)
                .append("]");
        return sb.toString();
    }

    /**
     * 时间戳存储到项目本地文件中
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
                    } else {
                        log.warn("oplog时间戳格式错误:{}", line);
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
