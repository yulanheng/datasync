## datasync是什么？
datasync是一个mongodb数据同步工具，从mongo replicaset的oplog中持续监听数据变化并通知消费者

## datasync怎么使用
datasync只提供一个轻微的监听内核，实现了持续监听oplog数据并通知消费者的能力。
你需要做的就是实现一个消费者(通过实现java.util.function.Consumer接口)，
然后利用这个消费者构造一个OPLogSyncTask实例即可，通过运行OPLogSyncTask实例
你自定义消费者将接收到来自datasync通知过来的数据

## datasync能做什么？
datasync虽然只实现了基本的监听功能，但通过强大的Consumer接口你可以实现任何想要的功能，
比如你可以实现一个KafkaConsumer用来将数据发往kafka。

## 原理
mongodb replicaset之间是使用oplog来实现复制集数据同步的，
而我们也正式通过监听oplog来感知mongodb数据的变化，
oplog存储在一个叫做oplog.rs的collection中，这个oplog.rs是一个特殊的collection，
因为他是一个固定大小的collection并且会循环覆盖数据，我们要做的就是从某个指定时间开始持续读取oplog.rs中的数据。

## 例子
下述例子演示了一个最简单的echo consumer的用法
```
Consumer<OPLogMessage> consumer = message -> {
    System.out.println("Recv:" + message);
};
OPLogSyncTask task = new OPLogSyncTask(collection, consumer);
task.start();
task.run();
```