package org.fire.datasync.task;

import com.google.common.collect.ImmutableMap;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * User: fire
 * Date: 2018-01-10
 */
public class MongoConfigProvider {
    private static final Logger logger = LoggerFactory.getLogger(MongoConfigProvider.class);
    private MongoClient clusterClient;

    public MongoConfigProvider(MongoClient clusterClient) {
        this.clusterClient = clusterClient;
    }

    public MongoConfig getConfig() {
        boolean isMongos = isMongos();
        List<MongoConfig.Shard> shards = getShards(isMongos);
        MongoConfig config = new MongoConfig(isMongos, shards);
        return config;
    }

    protected boolean ensureIsReplicaSet(MongoClient shardClient) {
        Set<String> collections = shardClient.getDB("local").getCollectionNames();
        if (!collections.contains("oplog.rs")) {
            throw new IllegalStateException("Cannot find oplog.rs collection. Please check this link: http://docs.mongodb.org/manual/tutorial/deploy-replica-set/");
        }
        return true;
    }


    private DB getAdminDb() {
        DB adminDb = clusterClient.getDB("admin");
        if (adminDb == null) {
            throw new RuntimeException("Could not get admin database from MongoDB");
        }
        return adminDb;
    }

    private DB getConfigDb() {
        DB configDb = clusterClient.getDB("config");
        if (configDb == null) {
            throw new RuntimeException("Could not get config database from MongoDB");
        }
        return configDb;
    }

    private boolean isMongos() {
        DB adminDb = getAdminDb();
        if (adminDb == null) {
            return false;
        }
        logger.trace("Found admin database");
        DBObject command = BasicDBObjectBuilder.start(
                ImmutableMap.builder().put("serverStatus", 1).put("asserts", 0).put("backgroundFlushing", 0).put("connections", 0)
                        .put("cursors", 0).put("dur", 0).put("extra_info", 0).put("globalLock", 0).put("indexCounters", 0)
                        .put("locks", 0).put("metrics", 0).put("network", 0).put("opcounters", 0).put("opcountersRepl", 0)
                        .put("recordStats", 0).put("repl", 0).build()).get();
        logger.trace("About to execute: {}", command);
        CommandResult cr = adminDb.command(command, ReadPreference.primary());
        logger.trace("Command executed return : {}", cr);

        logger.info("MongoDB version - {}", cr.get("version"));
        if (logger.isTraceEnabled()) {
            logger.trace("serverStatus: {}", cr);
        }

        if (!cr.ok()) {
            logger.warn("serverStatus returns error: {}", cr.getErrorMessage());
            return false;
        }

        if (cr.get("process") == null) {
            logger.warn("serverStatus.process return null.");
            return false;
        }
        String process = cr.get("process").toString().toLowerCase();
        if (logger.isTraceEnabled()) {
            logger.trace("process: {}", process);
        }
        // Fix for https://jira.mongodb.org/browse/SERVER-9160
        return (process.contains("mongos"));
//        }
    }

    private List<MongoConfig.Shard> getShards(boolean isMongos) {
        List<MongoConfig.Shard> shards = new ArrayList<>();
        if (isMongos) {
            try (DBCursor cursor = getConfigDb().getCollection("shards").find()) {
                while (cursor.hasNext()) {
                    DBObject item = cursor.next();
                    List<ServerAddress> shardServers = getServerAddressForReplica(item);
                    if (shardServers != null) {
                        String shardName = item.get("_id").toString();
                        MongoClient shardClient = getMongoShardClient(shardServers);
                        ensureIsReplicaSet(shardClient);
                        Timestamp<?> latestOplogTimestamp = getCurrentOplogTimestamp(shardClient);
                        shards.add(new MongoConfig.Shard(shardName, shardServers, latestOplogTimestamp));
                    }
                }
            }
            return shards;
        } else {
            ensureIsReplicaSet(clusterClient);
            List<ServerAddress> servers = clusterClient.getServerAddressList();
            Timestamp<?> latestOplogTimestamp = getCurrentOplogTimestamp(clusterClient);
            shards.add(new MongoConfig.Shard("unsharded", servers, latestOplogTimestamp));
            return shards;
        }
    }

    private List<ServerAddress> getServerAddressForReplica(DBObject item) {
        String definition = item.get("host").toString();
        if (definition.contains("/")) {
            definition = definition.substring(definition.indexOf("/") + 1);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("getServerAddressForReplica - definition: {}", definition);
        }
        List<ServerAddress> servers = new ArrayList<ServerAddress>();
        for (String server : definition.split(",")) {
            servers.add(new ServerAddress(server));
        }
        return servers;
    }

    private Timestamp<?> getCurrentOplogTimestamp(MongoClient shardClient) {
        DBCollection oplogCollection = shardClient
                .getDB("local")
                .getCollection("oplog.rs");
        try (DBCursor cursor = oplogCollection.find().sort(new BasicDBObject("$natural", -1)).limit(1)) {
            return Timestamp.on(cursor.next());
        }
    }

    public MongoClient getMongoShardClient(List<ServerAddress> shardServers) {
        List<ServerAddress> servers = shardServers;
        logger.info("Creating MongoClient for [{}]", servers);
        MongoClient mongoClient = new MongoClient(servers);
        return mongoClient;
    }
}
