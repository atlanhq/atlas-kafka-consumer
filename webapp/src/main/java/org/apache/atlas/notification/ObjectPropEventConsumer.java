package org.apache.atlas.notification;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import kafka.utils.ShutdownableThread;
import org.apache.atlas.*;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.notification.ObjectPropEvent;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityCorrelationStore;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.apache.atlas.service.Service;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsCounter;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2.*;

@Component
@Order(5)
public class ObjectPropEventConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG        = LoggerFactory.getLogger(ObjectPropEventConsumer.class);
    private static final Logger PERF_LOG   = AtlasPerfTracer.getPerfLogger(ObjectPropEventConsumer.class);
    private static final Logger FAILED_LOG = LoggerFactory.getLogger("FAILED");
    private static final Logger LARGE_MESSAGES_LOG = LoggerFactory.getLogger("LARGE_MESSAGES");

    private static final int    SC_OK          = 200;
    private static final int    SC_BAD_REQUEST = 400;

    private static final String THREADNAME_PREFIX = ObjectPropEventConsumer.class.getSimpleName();

    public static final String CONSUMER_THREADS_PROPERTY         = "atlas.notification.object_prop.numthreads";
    public static final String CONSUMER_RETRIES_PROPERTY         = "atlas.notification.object_prop.maxretries";
    public static final String CONSUMER_FAILEDCACHESIZE_PROPERTY = "atlas.notification.object_prop.failedcachesize";
    public static final String CONSUMER_RETRY_INTERVAL           = "atlas.notification.consumer.retry.interval";
    public static final String CONSUMER_MIN_RETRY_INTERVAL       = "atlas.notification.consumer.min.retry.interval";
    public static final String CONSUMER_MAX_RETRY_INTERVAL       = "atlas.notification.consumer.max.retry.interval";
    public static final String CONSUMER_COMMIT_BATCH_SIZE        = "atlas.notification.consumer.commit.batch.size";
    public static final String CONSUMER_DISABLED                 = "atlas.notification.consumer.disabled";
    public static final String CONSUMER_AUTHORIZE_USING_MESSAGE_USER                         = "atlas.notification.authorize.using.message.user";
    public static final String CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS                    = "atlas.notification.authorize.authn.cache.ttl.seconds";

    private final RedisService redisService;
    public static final int SERVER_READY_WAIT_TIME_MS = 1000;

    private final AtlasEntityStore atlasEntityStore;
    private final ServiceState serviceState;
    private final AtlasInstanceConverter instanceConverter;
    private final AtlasTypeRegistry typeRegistry;
    private final AtlasMetricsUtil metricsUtil;
    private final int                           maxRetries;
    private final int                           failedMsgCacheSize;
    private final int                           minWaitDuration;
    private final int                           maxWaitDuration;
    private final int                           commitBatchSize;
    private final int                           largeMessageProcessingTimeThresholdMs;
    private final boolean                       consumerDisabled;
    private final boolean createShellEntityForNonExistingReference;
    private final boolean                       authorizeUsingMessageUser;
    private final Map<String, Authentication>   authnCache;

    private final NotificationInterface         notificationInterface;
    private final Configuration applicationProperties;
    private ExecutorService executors;
    private Instant nextStatsLogTime = AtlasMetricsCounter.getNextHourStartTime(Instant.now());
    private final Map<TopicPartition, Long>     lastCommittedPartitionOffset;

    @VisibleForTesting
    final int consumerRetryInterval;

    @VisibleForTesting
    List<ObjectPropConsumer> consumers;

    private TransactionInterceptHelper   transactionInterceptHelper;
    @Inject
    public ObjectPropEventConsumer(NotificationInterface notificationInterface, AtlasEntityStore atlasEntityStore,
                                   ServiceState serviceState, AtlasInstanceConverter instanceConverter,
                                   AtlasTypeRegistry typeRegistry, AtlasMetricsUtil metricsUtil,
                                   EntityCorrelationStore entityCorrelationStore, RedisService redisService, TransactionInterceptHelper transactionInterceptHelper) throws AtlasException {
        this.notificationInterface = notificationInterface;
        this.atlasEntityStore      = atlasEntityStore;
        this.serviceState          = serviceState;
        this.instanceConverter     = instanceConverter;
        this.typeRegistry          = typeRegistry;
        this.redisService = redisService;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.applicationProperties = ApplicationProperties.get();
        this.metricsUtil                    = metricsUtil;
        this.lastCommittedPartitionOffset   = new HashMap<>();
        this.transactionInterceptHelper = transactionInterceptHelper;

        maxRetries            = applicationProperties.getInt(CONSUMER_RETRIES_PROPERTY, 3);
        failedMsgCacheSize    = applicationProperties.getInt(CONSUMER_FAILEDCACHESIZE_PROPERTY, 1);
        consumerRetryInterval = applicationProperties.getInt(CONSUMER_RETRY_INTERVAL, 500);
        minWaitDuration       = applicationProperties.getInt(CONSUMER_MIN_RETRY_INTERVAL, consumerRetryInterval); // 500 ms  by default
        maxWaitDuration       = applicationProperties.getInt(CONSUMER_MAX_RETRY_INTERVAL, minWaitDuration * 60);  //  30 sec by default
        commitBatchSize       = applicationProperties.getInt(CONSUMER_COMMIT_BATCH_SIZE, 50);

        consumerDisabled                              = applicationProperties.getBoolean(CONSUMER_DISABLED, false);
        largeMessageProcessingTimeThresholdMs         = applicationProperties.getInt("atlas.notification.consumer.large.message.processing.time.threshold.ms", 60 * 1000);  //  60 sec by default
        createShellEntityForNonExistingReference      = AtlasConfiguration.NOTIFICATION_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF.getBoolean();
        authorizeUsingMessageUser                     = applicationProperties.getBoolean(CONSUMER_AUTHORIZE_USING_MESSAGE_USER, false);

        int authnCacheTtlSeconds = applicationProperties.getInt(CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS, 300);

        authnCache = (authorizeUsingMessageUser && authnCacheTtlSeconds > 0) ? new PassiveExpiringMap<>(authnCacheTtlSeconds * 1000) : null;
        LOG.info("{}={}", CONSUMER_COMMIT_BATCH_SIZE, commitBatchSize);
        LOG.info("{}={}", CONSUMER_DISABLED, consumerDisabled);
    }

    @Override
    public void start() throws AtlasException {
        if (consumerDisabled) {
            LOG.info("No object_prop messages will be processed. {} = {}", CONSUMER_DISABLED, consumerDisabled);
            return;
        }

        startInternal(applicationProperties, null);
    }

    void startInternal(Configuration configuration, ExecutorService executorService) {
        if (consumers == null) {
            consumers = new ArrayList<>();
        }
        if (executorService != null) {
            executors = executorService;
        }
        if (!HAConfiguration.isHAEnabled(configuration)) {
            LOG.info("HA is disabled, starting consumers inline.");

            startConsumers(executorService);
        }
    }

    // TODO : POI #1 - init
    private void startConsumers(ExecutorService executorService) {
        int                                          numThreads            = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 1);
        List<NotificationConsumer<ObjectPropEvent>> notificationConsumers = notificationInterface.createConsumers(NotificationInterface.NotificationType.OBJECT_PROP_EVENTS, numThreads);

        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(notificationConsumers.size(), new ThreadFactoryBuilder().setNameFormat(THREADNAME_PREFIX + " thread-%d").build());
        }

        executors = executorService;

        for (final NotificationConsumer<ObjectPropEvent> consumer : notificationConsumers) {
            ObjectPropConsumer objectPropConsumer = new ObjectPropConsumer(consumer, transactionInterceptHelper);

            consumers.add(objectPropConsumer);
            executors.submit(objectPropConsumer);
        }
    }

    @Override
    public void stop() {
        //Allow for completion of outstanding work
        try {
            if (consumerDisabled) {
                return;
            }

            stopConsumerThreads();
            if (executors != null) {
                executors.shutdown();

                if (!executors.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }

                executors = null;
            }

            notificationInterface.close();
        } catch (InterruptedException e) {
            LOG.error("Failure in shutting down consumers");
        }
    }

    private void stopConsumerThreads() {
        LOG.info("==> stopConsumerThreads()");

        if (consumers != null) {
            for (ObjectPropConsumer consumer : consumers) {
                consumer.shutdown();
            }

            consumers.clear();
        }

        LOG.info("<== stopConsumerThreads()");
    }

    /**
     * Start Kafka consumer threads that read from Kafka topic when server is activated.
     * <p>
     * Since the consumers create / update entities to the shared backend store, only the active instance
     * should perform this activity. Hence, these threads are started only on server activation.
     */
    @Override
    public void instanceIsActive() {
        if (consumerDisabled) {
            return;
        }

        LOG.info("Reacting to active state: initializing Kafka consumers");

        startConsumers(executors);
    }

    /**
     * Stop Kafka consumer threads that read from Kafka topic when server is de-activated.
     * <p>
     * Since the consumers create / update entities to the shared backend store, only the active instance
     * should perform this activity. Hence, these threads are stopped only on server deactivation.
     */
    @Override
    public void instanceIsPassive() {
        if (consumerDisabled) {
            return;
        }

        LOG.info("Reacting to passive state: shutting down Kafka consumers.");

        stop();
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.NOTIFICATION_HOOK_CONSUMER.getOrder();
    }

    static class Timer {
        public void sleep(int interval) throws InterruptedException {
            Thread.sleep(interval);
        }
    }

    private List<String> trimAndPurge(String[] values, String defaultValue) {
        final List<String> ret;

        if (values != null && values.length > 0) {
            ret = new ArrayList<>(values.length);

            for (String val : values) {
                if (StringUtils.isNotBlank(val)) {
                    ret.add(val.trim());
                }
            }
        } else if (StringUtils.isNotBlank(defaultValue)) {
            ret = Collections.singletonList(defaultValue.trim());
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }

    static class AdaptiveWaiter {
        private final long increment;
        private final long maxDuration;
        private final long minDuration;
        private final long resetInterval;
        private       long lastWaitAt;

        @VisibleForTesting
        long waitDuration;

        public AdaptiveWaiter(long minDuration, long maxDuration, long increment) {
            this.minDuration   = minDuration;
            this.maxDuration   = maxDuration;
            this.increment     = increment;
            this.waitDuration  = minDuration;
            this.lastWaitAt    = 0;
            this.resetInterval = maxDuration * 2;
        }

        public void pause(Exception ex) {
            setWaitDurations();

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} in ObjectPropEventConsumer. Waiting for {} ms for recovery.", ex.getClass().getName(), waitDuration, ex);
                }

                Thread.sleep(waitDuration);
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} in ObjectPropEventConsumer. Waiting for recovery interrupted.", ex.getClass().getName(), e);
                }
            }
        }

        private void setWaitDurations() {
            long timeSinceLastWait = (lastWaitAt == 0) ? 0 : System.currentTimeMillis() - lastWaitAt;

            lastWaitAt = System.currentTimeMillis();

            if (timeSinceLastWait > resetInterval) {
                waitDuration = minDuration;
            } else {
                waitDuration += increment;
                if (waitDuration > maxDuration) {
                    waitDuration = maxDuration;
                }
            }
        }
    }

    @VisibleForTesting
    class ObjectPropConsumer extends ShutdownableThread {
        private final NotificationConsumer<ObjectPropEvent> consumer;
        private final AtomicBoolean shouldRun      = new AtomicBoolean(false);
        private final List<String>                           failedMessages = new ArrayList<>();
        private final TransactionInterceptHelper transactionInterceptHelper;
        private final ObjectPropEventConsumer.AdaptiveWaiter adaptiveWaiter = new ObjectPropEventConsumer.AdaptiveWaiter(minWaitDuration, maxWaitDuration, minWaitDuration);
        private String parentTaskGuid = "";

        public final String SUCCESS_SUB_TASKS_KEY = "task:" + parentTaskGuid + ":success";
        public final String FAILED_SUB_TASKS_KEY = "task:" + parentTaskGuid + ":failed";
        public final String PARENT_TASK_MAP = "task:" + parentTaskGuid;

        public ObjectPropConsumer(NotificationConsumer<ObjectPropEvent> consumer, TransactionInterceptHelper transactionInterceptHelper) {
            super("atlas-object_prop-consumer-thread", false);
            this.transactionInterceptHelper = transactionInterceptHelper;
            this.consumer = consumer;
        }

        @Override
        public void doWork() {
            // INC kafka batch size -> reduce kafka calls
            long startTime = System.currentTimeMillis();

            shouldRun.set(true);

            if (!serverAvailable(new ObjectPropEventConsumer.Timer())) {
                return;
            }
            LOG.info("ObjectPropConsumer::doWork() [Line 3] => serverAvailable() was true in {} ms",
                    (System.currentTimeMillis() - startTime));

            try {
                while (shouldRun.get()) {
                    long loopStart = System.currentTimeMillis();

                    try {
                        long receiveStart = System.currentTimeMillis();
                        List<AtlasKafkaMessage<ObjectPropEvent>> messages =
                                consumer.receiveWithCheckedCommit(lastCommittedPartitionOffset);
                        LOG.info("ObjectPropConsumer::doWork() [Line 5] => receiveWithCheckedCommit() completed in {} ms",
                                (System.currentTimeMillis() - receiveStart));

                        LOG.info("ObjectPropConsumer::doWork() -> Messages received: {}", messages.size());
                        Set<String> successfullSubtasks = new HashSet<>();
                        Set<String> failedSubtasks = new HashSet<>();
                        long msgStart;
                        AtlasKafkaMessage<ObjectPropEvent> last_msg = null;
                        for (AtlasKafkaMessage<ObjectPropEvent> msg : messages) {
                            msgStart = System.currentTimeMillis();
                            boolean res = atlasEntityStore.processTasks(msg.getMessage());
                            LOG.info("ObjectPropConsumer::doWork() [Line 7-b] => processTasks() completed in {} ms",
                                    (System.currentTimeMillis() - msgStart));

                            if (res) {
                                successfullSubtasks.add(msg.getMessage().getEventId());
                            } else {
                                LOG.info("ObjectPropConsumer::doWork() -> Message processing failed");
                                failedSubtasks.add(msg.getMessage().getEventId());
                            }
                            last_msg = msg;
                        }

                        long lineStart = System.currentTimeMillis();
                        if(messages.size() > 0) {
                            transactionInterceptHelper.intercept(); // only commit if msgSize is non 0
                            LOG.info("ObjectPropConsumer::doWork() -> transactionInterceptHelper.intercept() completed in {} ms",
                                    (System.currentTimeMillis() - lineStart));
                            this.parentTaskGuid = (String) messages.get(0).getMessage().getPayload().getOrDefault("parentTaskGuid","");


                            if (successfullSubtasks.size() > 0) {
                                redisService.addToSet(SUCCESS_SUB_TASKS_KEY, successfullSubtasks);
                                successfullSubtasks.clear();
                                LOG.info("ObjectPropConsumer::doWork() [Line 8] => incremented ASSETS_COUNT_PROPAGATED");
                            }

                            if (failedSubtasks.size() > 0) {
                                redisService.addToSet(FAILED_SUB_TASKS_KEY, failedSubtasks);
                                failedSubtasks.clear();
                                LOG.info("ObjectPropConsumer::doWork() [Line 9] => incremented ASSETS_PROPAGATION_FAILED_COUNT");
                            }

                            redisService.putInHash(PARENT_TASK_MAP, "lastModifiedAt", System.currentTimeMillis());
                            redisService.executeBatch();
                            long commitStart = System.currentTimeMillis();
                            long commitOffset = messages.get(messages.size() - 1).getOffset() + 1;
                            consumer.commit(last_msg.getTopicPartition(), commitOffset);
                            LOG.info("ObjectPropConsumer::doWork() [Line 7-c] => commit offset done in {} ms",
                                    (System.currentTimeMillis() - commitStart));
                        }

                    }
                    catch (IllegalStateException ex) {
                        LOG.error("ERROR -> ObjectPropConsumer::doWork() [Line 10] => caught IllegalStateException");
                        adaptiveWaiter.pause(ex);
                        LOG.error("ERROR -> ObjectPropConsumer::doWork() -> adaptiveWaiter.pause(ex) done.");
                    }
                    catch (Exception e) {
                        LOG.error("ERROR -> ObjectPropConsumer::doWork() [Line 11] => caught generic Exception");

                        if (shouldRun.get()) {
                            LOG.error("ERROR -> ObjectPropConsumer::doWork() -> Exception in ObjectPropEventConsumer", e);
                            adaptiveWaiter.pause(e);
                            LOG.error("ERROR -> ObjectPropConsumer::doWork() -> adaptiveWaiter.pause(e) done.");
                        } else {
                            LOG.error("ERROR -> ObjectPropConsumer::doWork() -> shouldRun is false, breaking out of loop.");
                            break;
                        }
                    }
                    finally {
                        LOG.info("ObjectPropConsumer::doWork() [Line 8] => whole whileLoop completed in {} ms",
                                (System.currentTimeMillis() - loopStart));
                    }
                }
            } finally {
                // [Line 12] Close consumer
                long closeStart = System.currentTimeMillis();
                if (consumer != null) {
                    LOG.info("ObjectPropConsumer::doWork() [Line 12] => closing NotificationConsumer");
                    consumer.close();
                    LOG.info("ObjectPropConsumer::doWork() -> consumer closed in {} ms",
                            (System.currentTimeMillis() - closeStart));
                }

                long endTime = System.currentTimeMillis();
                LOG.info("ObjectPropConsumer::doWork() [Line 13] <== Exiting doWork() after {} ms",
                        (endTime - startTime));
            }
        }

        boolean serverAvailable(ObjectPropEventConsumer.Timer timer) {
            try {
                while (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                    try {
                        LOG.info("Atlas Server is not ready. Waiting for {} milliseconds to retry...", SERVER_READY_WAIT_TIME_MS);

                        timer.sleep(SERVER_READY_WAIT_TIME_MS);
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting for Atlas Server to become ready, " + "exiting consumer thread.", e);

                        return false;
                    }
                }
            } catch (Throwable e) {
                LOG.info("Handled AtlasServiceException while waiting for Atlas Server to become ready, exiting consumer thread.", e);

                return false;
            }

            LOG.info("Atlas Server is ready, can start reading Kafka events.");

            return true;
        }

        @Override
        public void shutdown() {
            LOG.info("==> ObjectPropConsumer shutdown()");

            // handle the case where thread was not started at all
            // and shutdown called
            if (shouldRun.get() == false) {
                return;
            }

            super.initiateShutdown();

            shouldRun.set(false);

            if (consumer != null) {
                consumer.wakeup();
            }

            super.awaitShutdown();

            LOG.info("<== ObjectPropConsumer shutdown()");
        }
    }
}
