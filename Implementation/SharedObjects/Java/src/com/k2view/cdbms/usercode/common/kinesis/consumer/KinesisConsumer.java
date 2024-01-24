package com.k2view.cdbms.usercode.common.kinesis.consumer;

import com.k2view.cdbms.interfaces.pubsub.PayLoadBatchImpl;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.SupplierIterator;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoMessageSubscriber;
import com.k2view.fabric.common.io.basic.pubsub.PubSub;
import com.k2view.fabric.common.io.basic.pubsub.PubSubDefaults;
import com.k2view.fabric.common.io.basic.pubsub.PubSubRecord;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener.WorkerState;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.k2view.cdbms.usercode.common.kinesis.AWSClientBuilder.kinesisClientBuilder;

public class KinesisConsumer implements IoMessageSubscriber, PubSub {
    private final Log log = Log.a(KinesisConsumer.class);
    private final StreamIdentifier stream;
    private final String applicationName;
    private final AtomicBoolean shutdownTriggered = new AtomicBoolean(false);
    protected Iterable<RecordBatch> iterableBatch;
    protected SupplierIterator<RecordBatch> iteratorBatch;
    private final Properties props;
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final long pollTimeout;
    private final Consumer<Exception> propagateException;
    private final KCLApplication kclApplication;
    private final Map<String, RecordProcessorCheckpointer> shardCheckpointerMap = new HashMap<>();
    private final Object recordsLock = new Object();
    private PayLoadBatchImpl records;
    private final int maxPollRecords;

    public KinesisConsumer(Properties providerProps, Consumer<Exception> propagateProviderException) throws ExecutionException, InterruptedException {
        String streamName = providerProps.getProperty(PubSubDefaults.TOPIC.getName());
        this.props = new Properties();
        this.props.putAll(providerProps);
        try (KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(kinesisClientBuilder(props.getProperty("Host")))) {
            StreamDescriptionSummary streamDescriptionSummary = kinesisClient
                    .describeStreamSummary(DescribeStreamSummaryRequest.builder().streamName(streamName).build())
                    .get()
                    .streamDescriptionSummary();
            this.stream = StreamIdentifier.singleStreamInstance(Arn.fromString(streamDescriptionSummary.streamARN()));
            if (Util.isEmpty(this.props.getProperty(PubSubDefaults.GROUP_ID.getName()))) {
                throw new IllegalArgumentException("Application Name for Kinesis subscriber can't be empty!");
            }
            this.applicationName = providerProps.getProperty(PubSubDefaults.GROUP_ID.getName());
            this.pollTimeout = this.pollTimeout(this.props);
            this.initIterators();
            this.propagateException = propagateProviderException;
            KCLApplication.Builder kclAppBuilder = KCLApplication.builder()
                    .region(props.getProperty("Host"))
                    .applicationName(this.applicationName)
                    .streamIdentifier(this.stream)
                    .consumer(this)
                    .initialPositionInStream(
                            InitialPositionInStream
                                    .valueOf(KinesisConsumerDefaults.INITIAL_POSITION_IN_STREAM.get(props)))
                    .consumerType(
                            KCLApplication.ConsumerType
                                    .valueOf(KinesisConsumerDefaults.CONSUMER_TYPE.get(props)))
                    .cloudWatchMetricsEnabled(KinesisConsumerDefaults.CLOUD_WATCH_METRICS.get(props));
            if (props.containsKey(PubSubDefaults.MAX_POLL_RECORDS.getName())) {
                this.maxPollRecords = (int) props.get(PubSubDefaults.MAX_POLL_RECORDS.getName());
            } else {
                this.maxPollRecords = PubSubDefaults.MAX_POLL_RECORDS.get(null);
            }
            kclAppBuilder.maxRecordsInRequest(this.maxPollRecords);
            if (props.containsKey(PubSubDefaults.PARTITIONS.getName())) {
                List<String> shardIds = Arrays.stream(
                                props.get(PubSubDefaults.PARTITIONS.getName()).toString().split(","))
                        .collect(Collectors.toList());
                if (!shardIds.isEmpty() && !shardIds.get(0).equals("-1")) kclAppBuilder.shards(shardIds);
            }
            this.kclApplication = kclAppBuilder.build();
            this.records = new PayLoadBatchImpl();
        }
    }

    @SuppressWarnings("unchecked")
    private void initIterators() {
        this.iteratorBatch = new SupplierIterator(this::poll);
        this.iterableBatch = () -> this.iteratorBatch;
    }

    private long pollTimeout(Properties props) {
        Long timeout = (Long)props.get(PubSubDefaults.POLL_TIMEOUT.getName());
        if (timeout == -1L) {
            return Long.MAX_VALUE;
        } else {
            return timeout == 0L ? 1000L : timeout;
        }
    }

    private void prePoll() {
        if (autoCommit.get()) {
            try {
                this.checkpoint();
            } catch (Exception e) {
                log.error("Failed to checkpoint!");
                handleException(e);
                return;
            }
        }
        this.waitForKclInit();
    }

    public Iterable<Record> poll() {
        this.prePoll();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<PayLoadBatchImpl> future = executorService.submit(() -> {
            while (!this.shutdownTriggered.get()) {
                synchronized (this.recordsLock) {
                    if (!Util.isEmpty(this.records)) {
                        PayLoadBatchImpl res = (PayLoadBatchImpl) this.records.clone();
                        this.records = new PayLoadBatchImpl();
                        return res;
                    }
                }
                Util.sleep(500);
            }
            return null;
        });
        try {
            return future.get(pollTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Kinesis consumer has timed out after {}ms. Terminating..", this.pollTimeout);
            shutdownTriggered.set(true);
            return null;
        } catch (Exception e) {
            handleException(e);
            return null;
        }
    }

    private void waitForKclInit() {
        // TODO add timeout for init
        if (this.kclInitNotDone()) {
            if (!this.kclApplication.wasRunTriggered()) this.kclApplication.run();
            log.debug("Waiting for KCL application instance initialization (application={})..", this.applicationName);
        }
        while (this.kclInitNotDone() && !this.shutdownTriggered.get()) {
            Util.sleep(500);
        }
    }

    private boolean kclInitNotDone() {
        WorkerState schedulerState = this.kclApplication.getSchedulerState();
        final WorkerState[] notDoneStates = {
                WorkerState.INITIALIZING,
                WorkerState.CREATED,
                null
                };
        return Arrays.asList(notDoneStates).contains(schedulerState);
    }

    @Override
    public Iterable<RecordBatch> receiveBatch() {
        return this.iterableBatch;
    }

    @Override
    public long seek(String s, int i, long l) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public long endOffset(String s, int i) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public long position(String s, int i) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public void beginTransaction() {
        if (!this.autoCommit.get()) {
            throw new IllegalStateException("Transaction couldn't be opened, this session contains another opened transaction");
        } else {
            this.waitForKclInit();
            this.autoCommit.set(false);
        }
    }

    private void assertTransactional(String op) {
        if (this.autoCommit.get()) {
            throw new IllegalStateException(String.format("%s is not supported outside of transaction", op));
        }
    }

    @Override
    public void commit() throws ShutdownException, InvalidStateException {
        this.assertTransactional("commit");
        this.checkpoint();
        this.autoCommit.set(true);
    }

    private void checkpoint() throws ShutdownException, InvalidStateException {
        List<PreparedCheckpointer> preparedCheckpointers = new LinkedList<>();
        // First, prepare the checkpoints, and then, if no exception was thrown, checkpoint.
        // This is to avoid checkpointing only part of the shards in case of a failure.
        for (Map.Entry<String, RecordProcessorCheckpointer> entry : shardCheckpointerMap.entrySet()) {
            preparedCheckpointers.add(entry.getValue().prepareCheckpoint());
        }
        for (PreparedCheckpointer preparedCheckpointer : preparedCheckpointers) {
            preparedCheckpointer.checkpoint();
        }
        shardCheckpointerMap.clear();
    }

    @Override
    public void rollback() {
        this.assertTransactional("rollback");
        log.debug("In rollback - nothing to do, just skipping checkpointing..");
        this.shardCheckpointerMap.clear();
        this.autoCommit.set(true);
    }

    @Override
    public void close() {
        shutdownTriggered.set(true);
        kclApplication.shutdown();
        while (!WorkerState.SHUT_DOWN.equals(kclApplication.getSchedulerState())) {
            Util.sleep(500);
        }
        this.records.clear();
    }

    @Override
    public void abort() {
        shutdownTriggered.set(true);
    }

    protected void handleException(Exception e) {
        log.error("Terminating Kinesis subscriber due to an error: {}", e);
        this.close();
        propagateException.accept(e);
    }

    protected void removeCheckpointer(String shardId) {
        this.shardCheckpointerMap.remove(shardId);
    }

    public void processRecords(String shardId, ProcessRecordsInput processRecordsInput) {
        while (!this.shutdownTriggered.get()) {
            synchronized (recordsLock) {
                int remainingCapacity = this.maxPollRecords - this.records.size();
                if (remainingCapacity>= processRecordsInput.records().size()) {
                    processRecordsInput.records().forEach(rec -> {
                        log.debug("Processing record with sequenceNumber={}", rec.sequenceNumber());
                        PubSubRecord pubSubRecord = new PubSubRecord(
                                SdkBytes
                                        .fromByteBuffer(rec.data())
                                        .asUtf8String(),
                                rec
                                        .approximateArrivalTimestamp()
                                        .getEpochSecond());
                        pubSubRecord.put("partitionKey", rec.partitionKey());
                        pubSubRecord.put("sequenceNumber", rec.sequenceNumber());
                        pubSubRecord.put("stream", stream.streamName());
                        this.records.add(pubSubRecord);
                    });
                    this.shardCheckpointerMap.put(shardId, processRecordsInput.checkpointer());
                    return;
                } else {
                    log.debug("Blocking processing of {} new records because of MAX_POLL_RECORDS (remaining capacity in current batch={}), waiting for next batch..", this.records.size(), remainingCapacity);
                }
            }
            Util.sleep(500);
        }
    }
}

