package org.fire.datasync;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.fire.datasync.common.LifecycleManager;
import org.fire.datasync.executor.Schedulers;
import org.fire.datasync.executor.SingleThreadExecutor;
import org.fire.datasync.task.OPLogMessage;
import org.fire.datasync.task.OPLogSyncTask;

import java.util.function.Consumer;

/**
 * 该示例演示了一个简单的echo消费者是如何消费oplog数据的
 * User: fire
 * Date: 2018-01-13
 */
public class EchoDemo {
    public static void main(String[] args) throws Exception {
        MongoClient client = new MongoClient("127.0.0.1", 27017);
        DB db = client.getDB("local");
        DBCollection collection = db.getCollection("oplog.rs");

        // 将Schedulers添加到生命周期管理器中
        LifecycleManager.INSTANCE.add(Schedulers.INSTANCE);

        // 该消费者只是简单的将数据输出到控制台
        Consumer<OPLogMessage> consumer = message -> {
            System.out.println("Recv:" + message);
        };
        OPLogSyncTask task = new OPLogSyncTask(collection, consumer);
        // 将task添加到生命周期管理器中
        LifecycleManager.INSTANCE.add(task);

        SingleThreadExecutor executor = new SingleThreadExecutor("oplog", 1);
        executor.execute(task);
        // 将executor添加到生命周期管理器中
        LifecycleManager.INSTANCE.add(executor);

        // 启动监听
        LifecycleManager.INSTANCE.start();

        Thread.sleep(10000);

        // 关闭监听
        LifecycleManager.INSTANCE.stop();
    }
}
