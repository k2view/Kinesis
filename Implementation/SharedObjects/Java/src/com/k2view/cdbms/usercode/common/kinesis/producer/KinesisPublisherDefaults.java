package com.k2view.cdbms.usercode.common.kinesis.producer;

import java.util.Properties;

public enum KinesisPublisherDefaults {
    RECORDS_PER_REQUEST(500),
    MAX_RETRIES(3)
    ;

    private final Object defaultVal;

    KinesisPublisherDefaults(Object defaultVal) {
        this.defaultVal = defaultVal;
    }

    public String getName() {
        return this.name();
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Properties args) {
        if (args == null) {
            return (T) this.defaultVal;
        } else {
            Object value = args.get(this.getName());
            if (value == null) {
                value = args.get(this.getName().toLowerCase());
            }

            return (T) (value == null ? this.defaultVal : value);
        }
    }
}