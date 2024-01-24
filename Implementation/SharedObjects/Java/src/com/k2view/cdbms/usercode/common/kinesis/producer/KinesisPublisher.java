package com.k2view.cdbms.usercode.common.kinesis.producer;

import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoMessagePublisher;
import com.k2view.fabric.common.io.IoMessageSubscriber;
import com.k2view.fabric.common.io.basic.pubsub.PubSub;
import com.k2view.fabric.common.io.basic.pubsub.PubSubDefaults;
import com.k2view.fabric.common.io.basic.pubsub.PubSubRecord;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KinesisPublisher implements IoMessagePublisher, PubSub {
    private static final Log log = Log.a(KinesisPublisher.class);
    private static final String EXPLICIT_HASH_KEY_PARAM = "PARTITION";
    private final Properties props = new Properties();
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final KinesisAsyncClient client;
    private final String stream;
    private List<PutRecordsRequestEntry> recordsRequestEntryList;
    private Future<PutRecordsResponse> lastFuture;

    public KinesisPublisher(KinesisAsyncClient client, Properties props) {
        this.props.putAll(props);
        this.stream = props.getProperty(PubSubDefaults.TOPIC.getName());
        this.client = client;
    }

    @Override
    public IoMessageSubscriber.Record sendSync(Object message, Map<String, Object> params) {
        PutRecordRequest request = this.buildPutRequest(message, params);
        AtomicReference<PutRecordResponse> putRecordResponse = new AtomicReference<>();
        Util.rte(() -> {
            PutRecordResponse recordResponse = client.putRecord(request).get();
            putRecordResponse.set(recordResponse);
        });
        PubSubRecord pubsubRecord = new PubSubRecord(putRecordResponse.get().sequenceNumber());
        pubsubRecord.put("Shard Id", putRecordResponse.get().shardId());
        pubsubRecord.put("Sequence number", putRecordResponse.get().sequenceNumber());
        return pubsubRecord;
    }

    @Override
    public void send(Object message, Map<String, Object> params) throws ExecutionException, InterruptedException, KinesisPublisherException {
        if (!this.autoCommit.get()) {
            // TODO not a real transaction - is that fine?
            if (lastFuture != null && lastFuture.isDone()) {
                // Check last request status to avoid accumulating more data if it failed
                awaitAndHandleResponse();
            }
            int recordsPerRequest = this.recordsPerRequest();
            if (recordsRequestEntryList == null) recordsRequestEntryList = new ArrayList<>(recordsPerRequest);
            PutRecordRequest putRequest = buildPutRequest(message, params);

            PutRecordsRequestEntry entry = PutRecordsRequestEntry.builder()
                    .data(putRequest.data())
                    .explicitHashKey(putRequest.explicitHashKey())
                    .partitionKey(putRequest.partitionKey()).build();
            recordsRequestEntryList.add(entry);
            if (recordsRequestEntryList.size() == recordsPerRequest) {
                this.lastFuture = putRecordsAsync();
            }
        } else {
            // TODO - sendSync is slow - what can be improved?
            //  1. Make it an async PutRecord call while holding a Future queue? (as in a Kafka async transaction)
            //      If so, why wasn't this done in Kafka async non-transaction?
            //  2. Behave the same as in transaction (i.e. async PutRecords), and handle the last uncommitted batch on close?
            sendSync(message, params);
        }
    }

    private int recordsPerRequest() {
        final int maxRecordsPerRequest = 500;
        int recordsPerRequest = KinesisPublisherDefaults.RECORDS_PER_REQUEST.get(props);
        if (recordsPerRequest > maxRecordsPerRequest || recordsPerRequest < 1) {
            throw new IllegalArgumentException(
                    String.format("RECORDS_PER_REQUEST must be between 1 and %d.", maxRecordsPerRequest));
        }
        return recordsPerRequest;
    }

    private int maxRetries() {
        int maxRetries = KinesisPublisherDefaults.MAX_RETRIES.get(props);
        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid value %d for MAX_RETRIES", maxRetries));
        }
        return maxRetries;
    }

    private void awaitAndHandleResponse() throws ExecutionException, InterruptedException, KinesisPublisherException {
        PutRecordsResponse putRecordsResponse = lastFuture.get();
        this.lastFuture = null;
        if (putRecordsResponse.failedRecordCount() > 0) {
            log.error("Failure occurred in PutRecords request. Skipping records that were awaiting to be submitted in the next request.");
            log.debug("Failed records: {}", putRecordsResponse.records().stream().filter(rec -> rec.errorCode() != null));
            log.debug("Skipped records (after failure): {}", this.recordsRequestEntryList);
            throw new KinesisPublisherException(String.format("Failed to publish %d records to stream %s after %d attempts.",
                    putRecordsResponse.failedRecordCount(),
                    this.stream,
                    this.maxRetries())
            );
        }
    }

    private Future<PutRecordsResponse> putRecordsAsync() throws ExecutionException, InterruptedException, KinesisPublisherException {
        if (this.lastFuture != null) {
            this.awaitAndHandleResponse();
        }
        PutRecordsRequest.Builder requestBuilder = PutRecordsRequest.builder()
                .records(recordsRequestEntryList)
                .streamName(this.stream);
        // requestBuilder has a copy of records, we're good to clear it
        recordsRequestEntryList.clear();
        FutureTask<PutRecordsResponse> future = new FutureTask<>(() -> putRecords(requestBuilder));
        future.run();
        return future;
    }

    public PutRecordsResponse putRecords(PutRecordsRequest.Builder requestBuilder) {
        PutRecordsRequest request = requestBuilder.build();
        log.debug("Publishing {} records to kinesis stream '{}'", request.records().size(), request.streamName());
        AtomicReference<PutRecordsResponse> response = new AtomicReference<>();
        Util.rte(() -> response.set(client.putRecords(request).get()));
        int attempts = 0;
        int maxRetries = this.maxRetries();
        List<PutRecordsRequestEntry> requestRecords = request.records();
        int failedRecordCount = response.get().failedRecordCount();
        if (failedRecordCount == 0) {
            log.debug("Successfully published {} records to stream '{}'", request.records().size(), request.streamName());
        }
        while (failedRecordCount > 0 && attempts<maxRetries) {
            final List<PutRecordsRequestEntry> failedRecords = new ArrayList<>();
            final List<PutRecordsResultEntry> responseRecords = response.get().records();
            log.debug("Kinesis stream '{}' - Publish attempt failed: successful={}, failed={}",
                    request.streamName(),
                    responseRecords.size()- failedRecordCount,
                    failedRecordCount);
            for (int i = 0; i < responseRecords.size(); i++) {
                final PutRecordsRequestEntry requestRecord = requestRecords.get(i);
                final PutRecordsResultEntry responseRecord = responseRecords.get(i);
                if (responseRecord.errorCode() != null) {
                    failedRecords.add(requestRecord);
                    log.warn("Kinesis stream '{}' - failed to write record!", request.streamName());
                    log.warn("errorCode={}, errorMessage={}", responseRecord.errorCode(), responseRecord.errorMessage());
                }
            }
            requestRecords = failedRecords;
            requestBuilder.records(requestRecords);
            attempts++;
            log.warn("Kinesis stream '{}' - Retrying to publish {} failed records (attempt #{})", failedRecords.size(), attempts);
            Util.rte(() -> response.set(client.putRecords(requestBuilder.build()).get()));
        }
        return response.get();
    }

    private PutRecordRequest buildPutRequest(Object message, Map<String, Object> params) {
        String partitionKey = (String) params.get(PubSubDefaults.KEY.getName());
        Object explicitHashKey = params.get(EXPLICIT_HASH_KEY_PARAM) == null ?
                props.get(EXPLICIT_HASH_KEY_PARAM)
                : params.get(EXPLICIT_HASH_KEY_PARAM);
        String explicitHashKeyStr = ParamConvertor.toString(explicitHashKey);
        String updatedStream = (String) params.get(PubSubDefaults.TOPIC.getName());
        String recordStream = !Util.isEmpty(updatedStream) ? updatedStream : this.stream;
        byte[] dataAsBytes = message.toString().getBytes(StandardCharsets.UTF_8);
        SdkBytes sdkBytes = SdkBytes.fromByteArray(dataAsBytes);
        PutRecordRequest.Builder putRecordRequestBuilder = PutRecordRequest.builder();
        putRecordRequestBuilder.streamName(recordStream)
                .data(sdkBytes)
                .partitionKey(partitionKey);
        if (!Util.isEmpty(explicitHashKeyStr) && !explicitHashKeyStr.equals("-1")) {
            putRecordRequestBuilder.explicitHashKey(explicitHashKeyStr);
        }
        return putRecordRequestBuilder.build();
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
            this.autoCommit.set(false);
        }
    }

    @Override
    public void commit() throws ExecutionException, InterruptedException, KinesisPublisherException {
        this.assertTransactional("commit");
        this.lastFuture = putRecordsAsync();
        this.awaitAndHandleResponse();
        this.autoCommit.set(true);
    }

    @Override
    public void rollback() {
        this.assertTransactional("rollback");
        if (this.recordsRequestEntryList != null) this.recordsRequestEntryList.clear();
        this.autoCommit.set(true);
    }

    @Override
    public void close() {
        if (this.recordsRequestEntryList != null) this.recordsRequestEntryList.clear();
    }

    @Override
    public void abort() {
        if (this.lastFuture != null) {
            this.lastFuture.cancel(true);
            this.lastFuture=null;
            if (this.recordsRequestEntryList != null) this.recordsRequestEntryList.clear();
        }
    }

    private void assertTransactional(String op) {
        if (this.autoCommit.get()) {
            throw new IllegalStateException(String.format("%s is not supported outside of transaction", op));
        }
    }

    public static class KinesisPublisherException extends Exception {
        private final String message;
        KinesisPublisherException(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return "KinesisPublisherException{" +
                    "message='" + message + '\'' +
                    '}';
        }
    }
}