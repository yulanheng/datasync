package org.fire.datasync;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.fire.datasync.executor.Schedulers;
import org.fire.datasync.task.OPLogSyncTask;
import org.fire.datasync.task.RollingFileAppenderConsumer;

/**
 * User: fire
 * Date: 2018-01-13
 */
public class Main {
    public static void main(String[] args) throws Exception {
        MongoClient client = new MongoClient("172.20.176.143", 27017);
        DB db = client.getDB("local");
        DBCollection collection = db.getCollection("oplog.rs");
        RollingFileAppenderConsumer consumer = new RollingFileAppenderConsumer("local.oplog.rs.json");
        consumer.setMongoClient(client);
        OPLogSyncTask task = new OPLogSyncTask(collection, consumer);
        task.start();

        Thread.sleep(10000);

        task.stop();
        client.close();
        Schedulers.close();
        consumer.close();
    }
}
