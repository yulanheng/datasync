## 数据增量同步
1. 增量同步分为MySQL同步和MongoDB同步
2. MySQL同步采用canal开源程序解析binlog，MongoDB采用自定义方式解析oplog

## MongoDB同步步骤
1. 以tail方式从某个位置开始读取oplog.rs
2. 处理oplog(插入、更新、删除)
3. 插入操作的oplog直接写入文件
4. 更新、删除操作先根据条件查询原始记录再写入文件
5. 文件滚动触发hdfs转换、并触发kafka通知