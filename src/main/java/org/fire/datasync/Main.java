package org.fire.datasync;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.fire.datasync.common.LifecycleManager;
import org.fire.datasync.executor.Schedulers;
import org.fire.datasync.executor.SingleThreadExecutor;
import org.fire.datasync.task.DispatcherConsumer;
import org.fire.datasync.task.OPLogSyncTask;

/**
 * User: fire
 * Date: 2018-01-13
 */
public class Main {
    public static void main(String[] args) throws Exception {
        MongoClient client = new MongoClient("172.20.176.143", 27017);
        DB db = client.getDB("local");
        DBCollection collection = db.getCollection("oplog.rs");

        LifecycleManager.INSTANCE.add(Schedulers.INSTANCE);

        DispatcherConsumer consumer = new DispatcherConsumer();
        consumer.setMongoClient(client);
        LifecycleManager.INSTANCE.add(consumer);
        OPLogSyncTask task = new OPLogSyncTask("instance", collection, consumer);
        LifecycleManager.INSTANCE.add(task);
        SingleThreadExecutor executor = new SingleThreadExecutor("oplog", 1);
        executor.execute(task);
        LifecycleManager.INSTANCE.add(executor);

        LifecycleManager.INSTANCE.start();

        Thread.sleep(1000);

        LifecycleManager.INSTANCE.stop();
    }
}
