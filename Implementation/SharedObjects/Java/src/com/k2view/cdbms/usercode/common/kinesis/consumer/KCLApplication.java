package com.k2view.cdbms.usercode.common.kinesis.consumer;

import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.*;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener.WorkerState;
import software.amazon.kinesis.leases.ShardPrioritization;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.RetrievalSpecificConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.k2view.cdbms.usercode.common.kinesis.AWSClientBuilder.*;

class KCLApplication implements Runnable {

    public enum ShutdownMethod {
        NOW,
        GRACEFULLY
    }
    enum ConsumerType {
        SHARED_THROUGHPUT,
        ENHANCED_FAN_OUT,
    }
    private final Log log = Log.a(KCLApplication.class);
    private static final ShutdownMethod DEFAULT_SHUTDOWN_METHOD = ShutdownMethod.GRACEFULLY;
    private static final int SHUTDOWN_TIMEOUT_SEC = 20;

    private KinesisConsumer consumer;
    private Scheduler scheduler;
    private WorkerState schedulerState;
    private Thread schedulerThread;
    private final String applicationName;
    private final ShutdownMethod shutdownMethod;
    private final KinesisAsyncClient kinesisClient;
    private final CloudWatchAsyncClient cloudWatchClient;
    private final DynamoDbAsyncClient dynamoDbClient;

    static class Builder {
        private final Log log = Log.a(Builder.class);
        private String applicationName;
        private StreamIdentifier streamIdentifier;
        private InitialPositionInStream initialPositionInStream;
        private int maxRecordsInRequest;
        private List<String> shards;
        private KinesisConsumer consumer;
        private ConsumerType consumerType;
        private boolean cloudWatchMetricsEnabled;
        private String region;

        Builder() {}

        KCLApplication build() {
            log.debug("Building a new KCL Application instance (applicationName={},stream={})", applicationName, streamIdentifier.streamName());
            this.validateNonNull(this.applicationName, "applicationName");
            this.validateNonNull(this.streamIdentifier, "streamIdentifier");
            this.validateNonNull(this.initialPositionInStream, "initialPositionInStream");

            KinesisAsyncClient kinesisAsyncClient = KinesisClientUtil.adjustKinesisClientBuilder(kinesisClientBuilder(region)).build();
            DynamoDbAsyncClient dynamoDbAsyncClient = dynamoDbClientBuilder(region).build();
            CloudWatchAsyncClient cloudWatchAsyncClient = cloudWatchClientBuilder(region).build();
            ShardPrioritization shardPrioritization = this.shardPrioritization();
            MetricsFactory metricsFactory = this.metricsFactory();
            RetrievalSpecificConfig retrievalSpecificConfig = this.retrievalSpecificConfig(kinesisAsyncClient);
            KCLApplication kclApp = new KCLApplication(
                    applicationName + "_" + streamIdentifier.streamName(),
                    kinesisAsyncClient,
                    cloudWatchAsyncClient,
                    dynamoDbAsyncClient);
            ConfigsBuilder configsBuilder = new ConfigsBuilder(
                    streamIdentifier.streamName(),
                    applicationName + "_" + streamIdentifier.streamName(),
                    kinesisAsyncClient,
                    dynamoDbAsyncClient,
                    cloudWatchAsyncClient,
                    Util.fastUUID().toString(),
                    new KCLRecordProcessorFactory(applicationName, kclApp));
            RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig();
            MetricsConfig metricsConfig = configsBuilder.metricsConfig();
            if (retrievalSpecificConfig != null) retrievalConfig.retrievalSpecificConfig(retrievalSpecificConfig);
            if (metricsFactory != null) metricsConfig.metricsFactory(metricsFactory);
            kclApp.scheduler= new Scheduler(
                    configsBuilder.checkpointConfig(),
                    configsBuilder.coordinatorConfig()
                            .shardPrioritization(shardPrioritization)
                            .workerStateChangeListener(kclApp.getWorkerStateChangeListenerInstance()),
                    configsBuilder.leaseManagementConfig()
                            .initialPositionInStream(InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream)),
                    configsBuilder.lifecycleConfig(),
                    metricsConfig,
                    configsBuilder.processorConfig(),
                    retrievalConfig
                            .streamTracker(new SingleStreamTracker(streamIdentifier, InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream))));
            kclApp.consumer=consumer;
            return kclApp;
        }

        private ShardPrioritization shardPrioritization() {
            return list -> {
                if (!Util.isEmpty(shards)) {
                    return list
                            .stream()
                            .filter(shard -> shards.contains(shard.shardId()))
                            .collect(Collectors.toList());
                }
                return list;
            };
        }

        private MetricsFactory metricsFactory() {
            if (!cloudWatchMetricsEnabled) {
                return new NullMetricsFactory();
            }
            // If null, metricsFactory will be set by default to CloudWatchMetricsFactory
            return null;
        }

        private RetrievalSpecificConfig retrievalSpecificConfig(KinesisAsyncClient kinesisAsyncClient) {
            ConsumerType type = this.consumerType != null
                    ? this.consumerType : KinesisConsumerDefaults.CONSUMER_TYPE.get(new Properties());
            if (type.equals(ConsumerType.SHARED_THROUGHPUT)) {
                PollingConfig pollingConfig = new PollingConfig(streamIdentifier.streamName(), kinesisAsyncClient);
                if (this.maxRecordsInRequest > 0) {
                    pollingConfig.maxRecords(maxRecordsInRequest);
                }
                return pollingConfig;
            } else if (type.equals(ConsumerType.ENHANCED_FAN_OUT)) return null;
            // TO-DO set maxRecords for FanOutConfig
            //  https://github.com/awslabs/amazon-kinesis-client/issues/798
            // If retrievalSpecificConfig=null, it will be overridden by a FanOutConfig
            throw new IllegalArgumentException(
                    String.format("CONSUMER_TYPE in interface must be set to %s or %s",
                            ConsumerType.SHARED_THROUGHPUT,
                            ConsumerType.ENHANCED_FAN_OUT));
        }

        private void validateNonNull(Object arg, String argName) {
            if (arg==null) {
                throw new IllegalArgumentException(String.format("%s must not be null", argName));
            }
        }

        public Builder applicationName(String name) {
            this.applicationName=name;
            return this;
        }
        public Builder streamIdentifier(StreamIdentifier streamIdentifier) {
            this.streamIdentifier=streamIdentifier;
            return this;
        }
        public Builder consumer(KinesisConsumer consumer) {
            this.consumer = consumer;
            return this;
        }
        public Builder maxRecordsInRequest(int maxRecords) {
            this.maxRecordsInRequest = maxRecords;
            return this;
        }
        public Builder initialPositionInStream(InitialPositionInStream initialPositionInStream) {
            this.initialPositionInStream=initialPositionInStream;
            return this;
        }
        public Builder shards(List<String> shards) {
            this.shards = shards;
            return this;
        }
        public Builder consumerType(ConsumerType consumerType) {
            this.consumerType = consumerType;
            return this;
        }

        public Builder cloudWatchMetricsEnabled(boolean cloudWatchMetricsEnabled) {
            this.cloudWatchMetricsEnabled = cloudWatchMetricsEnabled;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }
    }

    private WorkerStateChangeListener getWorkerStateChangeListenerInstance() {
        return new KCLSchedulerStateChangeListener();
    }

    private KCLApplication(String applicationName, KinesisAsyncClient kinesisAsyncClient, CloudWatchAsyncClient cloudWatchAsyncClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.applicationName = applicationName;
        this.shutdownMethod = DEFAULT_SHUTDOWN_METHOD;
        this.kinesisClient = kinesisAsyncClient;
        this.cloudWatchClient = cloudWatchAsyncClient;
        this.dynamoDbClient = dynamoDbAsyncClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void run() {
        if (consumer==null) {
            throw new IllegalStateException(String.format("Failed to run KCL Application for application=%s: Please assign a consumer first", applicationName));
        }
        if (this.wasRunTriggered()) {
            throw new IllegalStateException(String.format("A KCL Application is already running for applicationName=%s. Application thread state is %s", applicationName, schedulerThread.getState()));
        }
        log.debug("Starting KCL Application instance for applicationName={}, consumer={}", applicationName, consumer);
        schedulerThread = Util.thread(
                String.format("KCLApplicationScheduler_%s", applicationName),
                scheduler);
    }

    void shutdown() {
        if (!schedulerState.equals(WorkerState.SHUT_DOWN_STARTED) && !schedulerState.equals(WorkerState.SHUT_DOWN)) {
            log.debug("Shutting down KCL Application instance for applicationName={}, consumer={}..", applicationName, consumer);
            try {
                Util.rte(() -> {
                    if (this.shutdownMethod.equals(ShutdownMethod.GRACEFULLY)) {
                        this.scheduler.startGracefulShutdown()
                                .exceptionally(throwable -> {
                                    log.error("Couldn't shutdown gracefully: {}", throwable);
                                    log.warn("Forcing shutdown..");
                                    scheduler.shutdown();
                                    return true;
                                })
                                .get(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
                    } else this.scheduler.shutdown();
                    log.debug("KCL Application shutdown complete for applicationName={}, consumer={}..", applicationName, consumer);
                });
            }
            finally {
                Util.safeClose(cloudWatchClient);
                Util.safeClose(dynamoDbClient);
                Util.safeClose(kinesisClient);
                this.consumer=null;
            }
        }
    }

    protected KinesisConsumer getConsumer() {
        return consumer;
    }

    protected synchronized boolean wasRunTriggered() {
        return this.schedulerThread != null;
    }

    protected WorkerState getSchedulerState() {
        return this.schedulerState;
    }

    private class KCLSchedulerStateChangeListener implements WorkerStateChangeListener {
        @Override
        public void onWorkerStateChange(WorkerState workerState) {
            schedulerState=workerState;
        }

        @Override
        public void onAllInitializationAttemptsFailed(Throwable e) {
            consumer.handleException(new KCLApplicationInitializationFailure(e));
        }
    }

    private static class KCLApplicationInitializationFailure extends RuntimeException {
        public KCLApplicationInitializationFailure(Throwable e) {
            super(e);
        }
    }
}


