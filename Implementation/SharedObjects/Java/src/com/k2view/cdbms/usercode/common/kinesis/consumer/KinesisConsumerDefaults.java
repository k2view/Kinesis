package com.k2view.cdbms.usercode.common.kinesis.consumer;

import software.amazon.kinesis.common.InitialPositionInStream;

import java.util.Properties;

public enum KinesisConsumerDefaults {
    INITIAL_POSITION_IN_STREAM(InitialPositionInStream.LATEST.name()),
    CONSUMER_TYPE(KCLApplication.ConsumerType.SHARED_THROUGHPUT.name()),
    CLOUD_WATCH_METRICS(false),
    ;

    private final Object defaultVal;

    KinesisConsumerDefaults(Object defaultVal) {
        this.defaultVal = defaultVal;
    }

    public String getName() {
        return this.name();
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Properties props) {
        if (props == null) {
            return (T) this.defaultVal;
        } else {
            Object value = props.get(this.getName());
            if (value == null) {
                value = props.get(this.getName().toLowerCase());
            }

            return (T) (value == null ? this.defaultVal : value);
        }
    }
}