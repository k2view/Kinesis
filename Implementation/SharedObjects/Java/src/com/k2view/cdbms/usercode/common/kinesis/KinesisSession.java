package com.k2view.cdbms.usercode.common.kinesis;

import com.k2view.cdbms.usercode.common.kinesis.consumer.KinesisConsumer;
import com.k2view.cdbms.usercode.common.kinesis.producer.KinesisPublisher;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.*;
import com.k2view.fabric.common.io.basic.pubsub.PubSub;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.*;
import java.util.stream.Collectors;

import static com.k2view.cdbms.usercode.common.kinesis.AWSClientBuilder.kinesisClientBuilder;

public class KinesisSession extends AbstractIoSession {
    private static final Log log = Log.a(KinesisSession.class);

    private boolean beginOnInit;
    private PubSub provider;
    private final Properties props = new Properties();
    private KinesisAsyncClient publisherClient;
    private Exception providerException;


    public KinesisSession(Properties props) {
        this.beginOnInit = false;
        if (!Util.isEmpty(props)) {
            this.props.putAll(props);
        }
    }

    @Override
    public IoMessagePublisher publisher(Map<String, Object> args) {
        log.debug("Creating a Kinesis publisher..");
        if (this.provider == null) {
            Map<String, Object> map = args.entrySet().stream().collect(Collectors.toMap(e ->
                    e.getKey().toUpperCase(), Map.Entry::getValue));
            this.props.putAll(map);
            if (this.publisherClient == null) this.publisherClient = kinesisClientBuilder(props.getProperty("Host")).build();
            this.provider = new KinesisPublisher(publisherClient, this.props);
            this.handleBeginOnInit();
        }

        return (IoMessagePublisher) this.provider;
    }

    private void handleBeginOnInit() {
        if (beginOnInit) {
            Util.rte(() -> {
                this.provider.beginTransaction();
                this.beginOnInit=false;
            });
        }
    }

    @Override
    public IoMessageSubscriber subscriber(Map<String, Object> input) {
        log.debug("Creating a Kinesis subscriber..");
        if (this.provider == null) {
            Map<String, Object> map = input.entrySet().stream().collect(Collectors.toMap(e ->
                    e.getKey().toUpperCase(), Map.Entry::getValue));
            this.props.putAll(map);
            Util.rte(() -> this.provider = new KinesisConsumer(this.props, e -> this.providerException=e));
        }
        this.handleBeginOnInit();
        return (IoMessageSubscriber) this.provider;
    }

    @Override
    public IoSessionCompartment compartment() {
        if ("__publisher".equals(this.props.get("_iosession_sub_identifier"))) return IoSessionCompartment.SHARED_PER_THREAD;
        return IoSessionCompartment.NOT_SHARED;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public void beginTransaction() throws Exception {
        log.debug("Starting Kinesis session transaction..");
        if (this.provider != null) {
            this.provider.beginTransaction();
        } else {
            this.beginOnInit=true;
        }
    }

    @Override
    public void commit() throws Exception {
        log.debug("Committing Kinesis session transaction..");
        this.provider.commit();
    }

    @Override
    public void rollback() throws Exception {
        log.debug("In Kinesis session rollback");
        this.provider.rollback();
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing Kinesis session..");
        Util.safeClose(provider);
        provider=null;
        Util.safeClose(publisherClient);
        publisherClient = null;
        if (this.providerException != null) {
            throw this.providerException;
        }
    }

    @Override
    public void abort() throws Exception {
        log.debug("Aborting Kinesis session..");
        if (this.provider != null) {
            this.provider.abort();
        }
    }
}
