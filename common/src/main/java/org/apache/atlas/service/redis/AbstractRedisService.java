package org.apache.atlas.service.redis;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.redisson.api.*;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public abstract class AbstractRedisService implements RedisService {

    private static final String REDIS_URL_PREFIX = "redis://";
    private static final String ATLAS_REDIS_URL = "atlas.redis.url";
    private static final String ATLAS_REDIS_SENTINEL_URLS = "atlas.redis.sentinel.urls";
    private static final String ATLAS_REDIS_USERNAME = "atlas.redis.username";
    private static final String ATLAS_REDIS_PASSWORD = "atlas.redis.password";
    private static final String ATLAS_REDIS_MASTER_NAME = "atlas.redis.master_name";
    private static final String ATLAS_REDIS_LOCK_WAIT_TIME_MS = "atlas.redis.lock.wait_time.ms";
    private static final String ATLAS_REDIS_LOCK_WATCHDOG_TIMEOUT_MS = "atlas.redis.lock.watchdog_timeout.ms";
    private static final int DEFAULT_REDIS_WAIT_TIME_MS = 15_000;
    private static final int DEFAULT_REDIS_LOCK_WATCHDOG_TIMEOUT_MS = 600_000;
    private static final String ATLAS_METASTORE_SERVICE = "atlas-metastore-service";

    private final ThreadLocal<RBatch> threadLocalBatch = new ThreadLocal<>();

    RedissonClient redisClient;
    RedissonClient redisCacheClient;
    Map<String, RLock> keyLockMap;
    Configuration atlasConfig;
    long waitTimeInMS;
    long watchdogTimeoutInMS;

    @Override
    public boolean acquireDistributedLock(String key) throws Exception {
        getLogger().info("Attempting to acquire distributed lock for {}, host:{}", key, getHostAddress());
        boolean isLockAcquired;
        try {
            RLock lock = redisClient.getFairLock(key);
            isLockAcquired = lock.tryLock(waitTimeInMS, TimeUnit.MILLISECONDS);
            if (isLockAcquired) {
                keyLockMap.put(key, lock);
            } else {
                getLogger().info("Attempt failed as lock {} is already acquired, host: {}.", key, getHostAddress());
            }
        } catch (InterruptedException e) {
            getLogger().error("Failed to acquire distributed lock for {}, host: {}", key, getHostAddress(), e);
            throw new AtlasException(e);
        }
        return isLockAcquired;
    }

    @Override
    public void releaseDistributedLock(String key) {
        if (!keyLockMap.containsKey(key)) {
            return;
        }
        try {
            RLock lock = keyLockMap.get(key);
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (Exception e) {
            getLogger().error("Failed to release distributed lock for {}", key, e);
        }
    }

    @Override
    public String getValue(String key) {
        // If value doesn't exist, return null else return the value
        return (String) redisCacheClient.getBucket(convertToNamespace(key)).get();
    }

    @Override
    public String putValue(String key, String value) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value);
        return value;
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value, timeout, TimeUnit.SECONDS);
        return value;
    }

    @Override
    public long incrValue(String key, long value) {
        RAtomicLong atomicLong = redisCacheClient.getAtomicLong(key);
        long newValue = atomicLong.addAndGet(value);
        return newValue;
    }

    @Override
    public long decrValue(String key, long value) {
        RAtomicLong atomicLong = redisCacheClient.getAtomicLong(key);
        long newValue = atomicLong.addAndGet(-1 * value);
        return newValue;
    }

    @Override
    public void removeValue(String key)  {
        // Remove the value from the redis cache
        redisCacheClient.getBucket(convertToNamespace(key)).delete();
    }

    public void beginBatch() {
        RBatch batch = redisClient.createBatch();
        threadLocalBatch.set(batch);
    }

    /**
     * Add multiple members to a set in Redis using batch.
     *
     * @param key    key of the Redis set
     * @param values values to be added
     */
    /**
     * Add multiple members to a set in Redis in a batched/pipelined fashion,
     * and block until execution completes (fully synchronous from the caller’s perspective).
     *
     * @param key    the key of the Redis set
     * @param values the values to add
     */
    // Try to merge all 3 calls
    public void addToSet(String key, Set<String> values) {
        RBatch batch = getCurrentBatchOrCreate(); // auto-creates if none
        String redisKey = convertToNamespace(key);
        RSetAsync<Object> rSet = batch.getSet(redisKey);
        for (String value : values) {
            rSet.addAsync(value);
        }
    }


    /**
     * Remove multiple members from a set in Redis in a batched/pipelined fashion,
     * fully synchronous from the caller’s perspective.
     *
     * @param key    the key of the Redis set
     * @param values the values to remove
     */
    public void removeFromSet(String key, Set<String> values) {
        String redisKey = convertToNamespace(key);

        RBatch batch = redisCacheClient.createBatch();
        RSetAsync<Object> setBatch = batch.getSet(redisKey);

        for (String value : values) {
            setBatch.removeAsync(value);
        }

        batch.execute();
    }

    /**
     * Retrieve all set members from Redis.
     * Typically just one call, so no need for pipeline/batch.
     *
     * @param key the key for the Redis set
     * @return a Set of all members
     */
    public Set<String> getSetMembers(String key) {
        String redisKey = convertToNamespace(key);
        // Standard synchronous fetch
        return redisCacheClient.<String>getSet(redisKey).readAll();
    }

    public void putAllInHash(String key, Map<String, String> entries) {
        RBatch batch = getCurrentBatchOrCreate();

        String redisKey = convertToNamespace(key);

        batch.getMap(redisKey).putAllAsync(entries);
    }

    /**
     * Puts a single field-value pair in a Redis hash, as part of the current batch pipeline.
     *
     * @param hashKey the Redis key (for the hash itself)
     * @param field   the field within the hash
     * @param value   the value to set for that field
     */
    public void putInHash(String hashKey, String field, Object value) {
        RBatch batch = getCurrentBatchOrCreate();
        String redisKey = convertToNamespace(hashKey);
        batch.getMap(redisKey).putAsync(field, value);
    }

    /**
     * Retrieves all fields (key-value pairs) from a Redis Hash as a Map.
     *
     * NOTE: This does not use batching because a single read call typically
     * doesn't benefit much from pipelining, and returning results from a
     * pipeline is more complex. This method is fully synchronous.
     */
    public Map<String, String> getHashAsMap(String hashKey) {
        String redisKey = convertToNamespace(hashKey);

        RMap<String, String> rMap = redisClient.getMap(redisKey);

        return rMap.readAllMap();
    }



    private RBatch getCurrentBatchOrCreate() {
        RBatch batch = threadLocalBatch.get();
        if (batch == null) {
            // Automatically create one
            batch = redisClient.createBatch();
            threadLocalBatch.set(batch);
        }
        return batch;
    }


    public void executeBatch() {
        RBatch batch = threadLocalBatch.get();
        if (batch != null) {
            try {
                batch.execute();
            } finally {
                // remove the batch from ThreadLocal to avoid reuse or memory leaks
                threadLocalBatch.remove();
            }
        } else {
            // No batch was started on this thread
            throw new IllegalStateException("No batch is active on this thread. " +
                    "Did you call beginBatch()?");
        }
    }

    private String getHostAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private Config initAtlasConfig() throws AtlasException {
        keyLockMap = new ConcurrentHashMap<>();
        atlasConfig = ApplicationProperties.get();
        waitTimeInMS = atlasConfig.getLong(ATLAS_REDIS_LOCK_WAIT_TIME_MS, DEFAULT_REDIS_WAIT_TIME_MS);
        watchdogTimeoutInMS = atlasConfig.getLong(ATLAS_REDIS_LOCK_WATCHDOG_TIMEOUT_MS, DEFAULT_REDIS_LOCK_WATCHDOG_TIMEOUT_MS);
        Config redisConfig = new Config();
        redisConfig.setLockWatchdogTimeout(watchdogTimeoutInMS);
        return redisConfig;
    }

    private String convertToNamespace(String key){
        // Append key with namespace :atlas
        return "atlas:"+key;
    }

    Config getLocalConfig() throws AtlasException {
        Config config = initAtlasConfig();
        config.useSingleServer()
                .setAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_URL))[0])
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD));
        return config;
    }

    Config getProdConfig() throws AtlasException {
        Config config = initAtlasConfig();
        config.useSentinelServers()
                .setClientName(ATLAS_METASTORE_SERVICE)
                .setReadMode(ReadMode.MASTER_SLAVE)
                .setCheckSentinelsList(false)
                .setKeepAlive(true)
                .setMasterConnectionMinimumIdleSize(10)
                .setMasterConnectionPoolSize(20)
                .setSlaveConnectionMinimumIdleSize(10)
                .setSlaveConnectionPoolSize(20)
                .setMasterName(atlasConfig.getString(ATLAS_REDIS_MASTER_NAME))
                .addSentinelAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_SENTINEL_URLS)))
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD));
        return config;
    }

    Config getCacheImplConfig() {
        Config config = new Config();
        config.useSentinelServers()
                .setClientName(ATLAS_METASTORE_SERVICE+"-redisCache")
                .setReadMode(ReadMode.MASTER_SLAVE)
                .setCheckSentinelsList(false)
                .setKeepAlive(true)
                .setMasterConnectionMinimumIdleSize(10)
                .setMasterConnectionPoolSize(20)
                .setSlaveConnectionMinimumIdleSize(10)
                .setSlaveConnectionPoolSize(20)
                .setMasterName(atlasConfig.getString(ATLAS_REDIS_MASTER_NAME))
                .addSentinelAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_SENTINEL_URLS)))
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD))
                .setTimeout(50) //Setting UP timeout to 50ms
                .setRetryAttempts(0);
        return config;
    }

    private String[] formatUrls(String[] urls) throws IllegalArgumentException {
        if (ArrayUtils.isEmpty(urls)) {
            getLogger().error("Invalid redis cluster urls");
            throw new IllegalArgumentException("Invalid redis cluster urls");
        }
        return Arrays.stream(urls).map(url -> {
            if (url.startsWith(REDIS_URL_PREFIX)) {
                return url;
            }
            return REDIS_URL_PREFIX + url;
        }).toArray(String[]::new);
    }

    @PreDestroy
    public void flushLocks(){
        keyLockMap.keySet().stream().forEach(k->keyLockMap.get(k).unlock());
    }
}
