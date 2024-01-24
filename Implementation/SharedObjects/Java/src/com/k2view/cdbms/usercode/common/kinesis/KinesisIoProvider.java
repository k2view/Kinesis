package com.k2view.cdbms.usercode.common.kinesis;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

import java.util.Map;
import java.util.Properties;

public class KinesisIoProvider implements IoProvider {
    @Override
    public IoSession createSession(String identifier, Map<String, Object> params) {
        Properties props = new Properties();
        Map<String,Object> data = Json.get().fromJson(params.get("Data").toString());
        if (!Util.isEmpty(data)) {
            @SuppressWarnings("unchecked") Map<String,Object> consumerData = (Map<String, Object>) data.get("consumer");
            @SuppressWarnings("unchecked") Map<String,Object> publisherData = (Map<String, Object>) data.get("publisher");
            this.fillProps(consumerData, props);
            this.fillProps(publisherData, props);
        }
        this.fillProps(params, props);
        return new KinesisSession(props);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IoProvider> T unwrap(Class<T> clz) {
        return (T) this;
    }

    private void fillProps(Map<String,Object> data, Properties props) {
        if (!Util.isEmpty(data)) {
            props.putAll(data);
        }
    }
}
